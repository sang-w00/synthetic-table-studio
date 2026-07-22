from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sts.domain import ColumnKind, ColumnRole, ColumnSchema, DomainError, ErrorCode
from sts.rules.compiler import compile_rules
from sts.rules.execution import (
    audit_and_filter_source,
    audit_source,
    full_validate,
    mask_prefix,
    prepare_model_batch,
    reconstruct_batch,
)
from sts.rules.models import (
    AllowedValuesRule,
    CompareRule,
    ComparisonPredicate,
    ConditionalSetRule,
    FixedCombinationRule,
    IsNullPredicate,
    MaskPrefixRule,
    NotNullRule,
    RangeRule,
    RuleProvenance,
    SourceAction,
    SumEqualsRule,
)
from sts.rules.rejection import CandidateBatch, GlobalRejectionCoordinator, plan_shards

PUBLIC = RuleProvenance.PUBLIC


def column(
    name: str,
    kind: ColumnKind,
    *,
    nullable: bool = False,
    decimal_places: int | None = None,
    timezone: str | None = None,
) -> ColumnSchema:
    return ColumnSchema(
        name=name,
        kind=kind,
        nullable=nullable,
        role=ColumnRole.MODEL,
        decimal_places=decimal_places,
        timezone=timezone,
    )


def test_source_audit_drop_union_overlap_and_nullable_allowed_values(
    tmp_path: Path,
) -> None:
    columns = (
        column("number", ColumnKind.INTEGER, nullable=True),
        column("code", ColumnKind.CATEGORICAL, nullable=True),
    )
    rules = (
        NotNullRule(
            id="number-required",
            provenance=PUBLIC,
            source_action=SourceAction.DROP_ROW,
            column="number",
        ),
        AllowedValuesRule(
            id="known-code",
            provenance=PUBLIC,
            source_action=SourceAction.DROP_ROW,
            column="code",
            values=("ok",),
        ),
    )
    compiled = compile_rules(columns, rules)
    source = pa.table(
        {
            "number": pa.array([None, None, 1, 2, 3], type=pa.int64()),
            "code": pa.array(["bad", "ok", "bad", None, "ok"], type=pa.string()),
        }
    )
    source_path = tmp_path / "source.parquet"
    output_path = tmp_path / "filtered.parquet"
    pq.write_table(source, source_path)

    result = audit_and_filter_source(source_path, output_path, compiled, batch_rows=2)

    assert result.report.source_rows == 5
    assert result.report.retained_rows == 2
    assert result.report.per_rule_violations == {"known-code": 2, "number-required": 2}
    assert result.report.drop_union_count == 3
    assert result.report.drop_overlap_count == 1
    assert result.report.violation_union_count == 3
    assert result.report.violation_overlap_count == 1
    assert pq.read_table(output_path).to_pydict() == {
        "number": [2, 3],
        "code": [None, "ok"],
    }


def test_blocking_source_audit_prevents_destructive_output(tmp_path: Path) -> None:
    columns = (column("value", ColumnKind.INTEGER),)
    compiled = compile_rules(
        columns,
        (
            RangeRule(
                id="bounded",
                provenance=PUBLIC,
                source_action=SourceAction.BLOCK,
                column="value",
                min=0,
                max=10,
            ),
        ),
    )
    output_path = tmp_path / "must-not-exist.parquet"

    with pytest.raises(DomainError) as caught:
        audit_and_filter_source(pa.table({"value": [1, 11]}), output_path, compiled)

    assert caught.value.code is ErrorCode.SOURCE_RULE_VIOLATION
    assert caught.value.problem.context["per_rule_violations"] == {"bounded": 1}
    assert caught.value.problem.context["block_union_count"] == 1
    assert not output_path.exists()
    assert not list(tmp_path.glob("*.part"))


def test_audit_reports_block_rule_overlap_without_short_circuit() -> None:
    columns = (column("value", ColumnKind.INTEGER, nullable=True),)
    compiled = compile_rules(
        columns,
        (
            NotNullRule(id="nonnull", provenance=PUBLIC, column="value"),
            AllowedValuesRule(
                id="allowed", provenance=PUBLIC, column="value", values=(1,)
            ),
        ),
    )

    report = audit_source(
        pa.table({"value": pa.array([None, 2, 1], type=pa.int64())}), compiled
    ).report

    # Nullable null is allowed by allowed_values; only not_null rejects it.
    assert report.per_rule_violations == {"allowed": 1, "nonnull": 1}
    assert report.block_union_count == 2
    assert report.block_overlap_count == 0


def test_mask_prefix_counts_unicode_codepoints_and_preserves_nulls() -> None:
    columns = (column("text", ColumnKind.TEXT, nullable=True),)
    compiled = compile_rules(
        columns,
        (MaskPrefixRule(id="mask", provenance=PUBLIC, column="text", keep_chars=2),),
    )
    source = pa.table({"text": ["A😀é", "e\u0301x", "短", None]})

    prepared = prepare_model_batch(source, compiled)

    assert prepared.column("text").to_pylist() == ["A😀*", "e\u0301*", "短", None]
    assert mask_prefix("😀ab", 1) == "😀**"
    assert mask_prefix(None, 1) is None


def test_fixed_combination_latent_round_trip_with_explicit_null_sentinel() -> None:
    columns = (
        column("left", ColumnKind.CATEGORICAL, nullable=True),
        column("right", ColumnKind.BOOLEAN, nullable=True),
    )
    rule = FixedCombinationRule(
        id="pair",
        provenance=PUBLIC,
        columns=("left", "right"),
        allowed_tuples=(("a", True), (None, False)),
    )
    compiled = compile_rules(columns, (rule,))
    source = pa.table(
        {
            "left": pa.array(["a", None], type=pa.string()),
            "right": pa.array([True, False], type=pa.bool_()),
        }
    )

    prepared = prepare_model_batch(source, compiled)
    assert "left" not in prepared.column_names
    assert "right" not in prepared.column_names
    assert prepared.num_columns == 1
    reconstructed = reconstruct_batch(prepared, compiled)
    checked = full_validate(reconstructed, compiled)

    assert checked.report.violation_union_count == 0
    assert checked.table.to_pydict() == source.to_pydict()


def test_private_fixed_combination_domain_is_inferred_after_drop_union(
    tmp_path: Path,
) -> None:
    columns = (
        column("a", ColumnKind.CATEGORICAL),
        column("b", ColumnKind.CATEGORICAL),
        column("keep", ColumnKind.INTEGER),
    )
    fixed = FixedCombinationRule(
        id="private-pair",
        provenance=RuleProvenance.PRIVATE_INFERRED,
        columns=("a", "b"),
        allowed_tuples=None,
    )
    drop = AllowedValuesRule(
        id="keep-only",
        provenance=PUBLIC,
        source_action=SourceAction.DROP_ROW,
        column="keep",
        values=(1,),
    )
    compiled = compile_rules(columns, (fixed, drop))
    source = pa.table({"a": ["x", "secret"], "b": ["y", "tuple"], "keep": [1, 0]})

    result = audit_and_filter_source(source, tmp_path / "filtered.parquet", compiled)

    assert result.codecs.fixed_tuples["private-pair"] == (("x", "y"),)
    prepared = prepare_model_batch(
        pq.read_table(result.output_path), compiled, codecs=result.codecs
    )
    checked = full_validate(
        reconstruct_batch(prepared, compiled, codecs=result.codecs),
        compiled,
        codecs=result.codecs,
    )
    assert checked.report.violation_union_count == 0


def test_conditional_overwrite_and_null_predicate_semantics() -> None:
    columns = (
        column("flag", ColumnKind.INTEGER, nullable=True),
        column("target", ColumnKind.CATEGORICAL, nullable=True),
    )
    comparison = ConditionalSetRule(
        id="set-on-one",
        provenance=PUBLIC,
        when=ComparisonPredicate(column="flag", op="eq", value=1),
        target="target",
        value="forced",
    )
    compared = compile_rules(columns, (comparison,))
    source = pa.table(
        {
            "flag": pa.array([1, 0, None], type=pa.int64()),
            "target": ["wrong", "keep", "null-condition-kept"],
        }
    )

    reconstructed = reconstruct_batch(source, compared)
    assert reconstructed.column("target").to_pylist() == [
        "forced",
        "keep",
        "null-condition-kept",
    ]

    is_null = ConditionalSetRule(
        id="set-on-null",
        provenance=PUBLIC,
        when=IsNullPredicate(column="flag"),
        target="target",
        value="null-forced",
    )
    null_plan = compile_rules(columns, (is_null,))
    null_reconstructed = reconstruct_batch(source, null_plan)
    assert null_reconstructed.column("target").to_pylist() == [
        "wrong",
        "keep",
        "null-forced",
    ]


def test_sum_reconstruction_uses_scaled_int_half_even_and_nullable_null() -> None:
    columns = (
        column("first", ColumnKind.FIXED_DECIMAL, nullable=True, decimal_places=2),
        column("second", ColumnKind.FIXED_DECIMAL, nullable=True, decimal_places=2),
        column("total", ColumnKind.FIXED_DECIMAL, nullable=True, decimal_places=2),
    )
    compiled = compile_rules(
        columns,
        (
            SumEqualsRule(
                id="sum",
                provenance=PUBLIC,
                sources=("first", "second"),
                target="total",
                tolerance=Decimal("0.01"),
            ),
        ),
    )
    # Model-side values can carry more precision than the declared output dtype.
    candidates = pa.table(
        {
            "first": [Decimal("1.005"), Decimal("1.015"), None],
            "second": [Decimal("2.005"), Decimal("2.005"), Decimal("9.99")],
        }
    )

    reconstructed = reconstruct_batch(candidates, compiled)
    checked = full_validate(reconstructed, compiled)

    assert checked.table.column("total").to_pylist() == [
        Decimal("3.00"),
        Decimal("3.02"),
        None,
    ]
    assert checked.table.schema.field("total").type == pa.decimal128(38, 2)
    assert checked.report.violation_union_count == 0


def test_sum_reconstruction_rejects_int64_overflow() -> None:
    columns = (
        column("first", ColumnKind.INTEGER),
        column("second", ColumnKind.INTEGER),
        column("total", ColumnKind.INTEGER),
    )
    compiled = compile_rules(
        columns,
        (
            SumEqualsRule(
                id="sum",
                provenance=PUBLIC,
                sources=("first", "second"),
                target="total",
            ),
        ),
    )

    with pytest.raises(DomainError) as caught:
        reconstruct_batch(
            pa.table({"first": [2**63 - 1], "second": [1]}),
            compiled,
        )

    assert caught.value.code is ErrorCode.OUTPUT_INVALID
    assert caught.value.problem.context["reason"] == "int64_overflow"


@pytest.mark.parametrize(
    ("op", "expected_right"),
    (("<", 6), ("<=", 5), (">", 4), (">=", 5)),
)
def test_integer_compare_anchor_delta_strict_and_nonstrict(
    op: str, expected_right: int
) -> None:
    columns = (column("left", ColumnKind.INTEGER), column("right", ColumnKind.INTEGER))
    source_right = 8 if op in {"<", "<="} else 2
    compiled = compile_rules(
        columns,
        (
            CompareRule(
                id="ordered", provenance=PUBLIC, left="left", op=op, right="right"
            ),
        ),
    )
    prepared = prepare_model_batch(
        pa.table({"left": [5], "right": [source_right]}), compiled
    )
    latent = next(
        name for name in prepared.column_names if name.startswith("__sts_rule_")
    )
    candidate = prepared.set_column(
        prepared.schema.get_field_index(latent), latent, pa.array([0])
    )

    checked = full_validate(reconstruct_batch(candidate, compiled), compiled)

    assert checked.table.column("right").to_pylist() == [expected_right]
    assert checked.report.violation_union_count == 0


@pytest.mark.parametrize(
    ("kind", "left", "right", "expected"),
    (
        (ColumnKind.DATE, date(2025, 1, 1), date(2025, 1, 3), date(2025, 1, 2)),
        (
            ColumnKind.DATETIME,
            datetime(2025, 1, 1, tzinfo=UTC),
            datetime(2025, 1, 1, 0, 0, 0, 2, tzinfo=UTC),
            datetime(2025, 1, 1, 0, 0, 0, 1, tzinfo=UTC),
        ),
        (ColumnKind.FIXED_DECIMAL, Decimal("5.00"), Decimal("5.02"), Decimal("5.01")),
    ),
)
def test_compare_reconstruction_uses_kind_precision(
    kind: ColumnKind, left: object, right: object, expected: object
) -> None:
    kwargs = {
        "decimal_places": 2 if kind is ColumnKind.FIXED_DECIMAL else None,
        "timezone": "UTC" if kind is ColumnKind.DATETIME else None,
    }
    columns = (column("left", kind, **kwargs), column("right", kind, **kwargs))
    compiled = compile_rules(
        columns,
        (
            CompareRule(
                id="ordered", provenance=PUBLIC, left="left", op="<", right="right"
            ),
        ),
    )
    prepared = prepare_model_batch(
        pa.table({"left": [left], "right": [right]}), compiled
    )
    latent = next(
        name for name in prepared.column_names if name.startswith("__sts_rule_")
    )
    candidate = prepared.set_column(
        prepared.schema.get_field_index(latent), latent, pa.array([0])
    )

    checked = full_validate(reconstruct_batch(candidate, compiled), compiled)

    assert checked.table.column("right").to_pylist() == [expected]
    assert checked.report.violation_union_count == 0


def test_float_compare_uses_public_granularity_as_strict_unit() -> None:
    columns = (column("left", ColumnKind.FLOAT), column("right", ColumnKind.FLOAT))
    compiled = compile_rules(
        columns,
        (
            CompareRule(
                id="float-order",
                provenance=PUBLIC,
                left="left",
                op="<",
                right="right",
                granularity=0.25,
            ),
        ),
    )
    prepared = prepare_model_batch(pa.table({"left": [1.0], "right": [2.0]}), compiled)
    latent = next(
        name for name in prepared.column_names if name.startswith("__sts_rule_")
    )
    candidate = prepared.set_column(
        prepared.schema.get_field_index(latent), latent, pa.array([0.0])
    )

    checked = full_validate(reconstruct_batch(candidate, compiled), compiled)

    assert checked.table.column("right").to_pylist() == [1.25]
    assert checked.report.violation_union_count == 0


def test_full_validator_covers_allowed_range_not_null_and_exact_sum() -> None:
    columns = (
        column("value", ColumnKind.INTEGER, nullable=True),
        column("part", ColumnKind.INTEGER),
        column("total", ColumnKind.INTEGER),
    )
    compiled = compile_rules(
        columns,
        (
            NotNullRule(id="nonnull", provenance=PUBLIC, column="value"),
            AllowedValuesRule(
                id="allowed", provenance=PUBLIC, column="value", values=(1, 2)
            ),
            RangeRule(id="range", provenance=PUBLIC, column="value", min=1, max=2),
            SumEqualsRule(
                id="sum",
                provenance=PUBLIC,
                sources=("part",),
                target="total",
                tolerance=Decimal("1"),
            ),
        ),
    )
    output = pa.table(
        {
            "value": pa.array([None, 3, 1], type=pa.int64()),
            "part": [1, 1, 1],
            "total": [1, 1, 2],
        }
    )

    checked = full_validate(output, compiled)

    assert checked.report.per_rule_violations == {
        "allowed": 1,
        "nonnull": 1,
        "range": 1,
        "sum": 1,
    }
    assert checked.report.violation_union_count == 3
    assert checked.report.invalid_rows == (True, True, True)


def test_global_multi_shard_budget_exact_output_and_zero_post_violations(
    tmp_path: Path,
) -> None:
    columns = (column("value", ColumnKind.INTEGER),)
    compiled = compile_rules(
        columns,
        (RangeRule(id="binary", provenance=PUBLIC, column="value", min=0, max=1),),
    )
    coordinator = GlobalRejectionCoordinator(
        compiled,
        output_rows=4,
        shard_count=2,
        master_seed=123456,
    )

    def provider(allocation):
        value = 9 if allocation.shard_id == 0 else 1
        return [
            CandidateBatch(
                candidate_start=allocation.candidate_start,
                batch=pa.table({"value": [value] * allocation.candidate_quota}),
            )
        ]

    output_path = tmp_path / "synthetic.parquet"
    result = coordinator.run(provider, output_path)

    assert result.actual_rows == 4
    assert result.post_violations == 0
    assert result.candidates_examined == 80
    assert result.candidates_rejected == 40
    assert pq.read_table(output_path).column("value").to_pylist() == [1, 1, 1, 1]
    first, second = result.allocations
    assert first.candidate_stop == second.candidate_start
    assert sum(allocation.candidate_quota for allocation in result.allocations) == 80
    assert plan_shards(4, 2, 123456) == result.allocations
    assert first.seed != second.seed


def test_global_budget_exhaustion_publishes_no_artifact(tmp_path: Path) -> None:
    columns = (column("value", ColumnKind.INTEGER),)
    compiled = compile_rules(
        columns,
        (RangeRule(id="binary", provenance=PUBLIC, column="value", min=0, max=1),),
    )
    coordinator = GlobalRejectionCoordinator(
        compiled,
        output_rows=2,
        shard_count=2,
        master_seed=9,
    )

    def invalid_provider(allocation):
        return [
            CandidateBatch(
                candidate_start=allocation.candidate_start,
                batch=pa.table({"value": [7] * allocation.candidate_quota}),
            )
        ]

    output_path = tmp_path / "not-published.parquet"
    with pytest.raises(DomainError) as caught:
        coordinator.run(invalid_provider, output_path)

    assert caught.value.code is ErrorCode.RULE_FEASIBILITY_EXHAUSTED
    assert caught.value.problem.context == {
        "requested_rows": 2,
        "accepted_rows": 0,
        "candidates_examined": 40,
        "max_candidates": 40,
    }
    assert not output_path.exists()
    assert not list(tmp_path.glob("*.part"))
