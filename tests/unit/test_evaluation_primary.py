from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from sts.domain import (
    ColumnKind,
    ColumnRole,
    ColumnSchema,
    DomainError,
    ErrorCode,
    IdentifierStrategy,
)
from sts.evaluation import (
    EvaluationConfig,
    deterministic_hmac_sample,
    evaluate_primary,
    exact_full_scan,
)


def model_column(
    name: str,
    kind: ColumnKind,
    *,
    nullable: bool = False,
    public_bins: tuple[int, ...] | None = None,
    public_categories: tuple[str, ...] | None = None,
) -> ColumnSchema:
    return ColumnSchema(
        name=name,
        kind=kind,
        nullable=nullable,
        role=ColumnRole.MODEL,
        public_bins=public_bins,
        public_categories=public_categories,
    )


def identifier_column(name: str, *, pattern: str | None = None) -> ColumnSchema:
    return ColumnSchema(
        name=name,
        kind=ColumnKind.IDENTIFIER,
        nullable=False,
        role=ColumnRole.IDENTIFIER,
        identifier_strategy=IdentifierStrategy.SEQUENTIAL,
        format=pattern,
    )


def primary_fixture() -> tuple[pa.Table, pa.Table, pa.Table, tuple[ColumnSchema, ...]]:
    columns = (
        model_column("number", ColumnKind.INTEGER, nullable=True),
        model_column("category", ColumnKind.CATEGORICAL),
        model_column("constant_same", ColumnKind.CATEGORICAL),
        model_column("constant_different", ColumnKind.CATEGORICAL),
        model_column("all_null", ColumnKind.FLOAT, nullable=True),
        identifier_column("record_id"),
        model_column("free_text", ColumnKind.TEXT),
    )
    ids = ["ID-001", "ID-002", "ID-003", "ID-004"]
    train = pa.table(
        {
            "__sts_row_id": pa.array([0, 1, 2, 3], type=pa.int64()),
            "number": pa.array([0, 1, 2, None], type=pa.int64()),
            "category": ["a", "a", "b", "b"],
            "constant_same": ["x"] * 4,
            "constant_different": ["x"] * 4,
            "all_null": pa.array([None] * 4, type=pa.float64()),
            "record_id": ids,
            "free_text": ["one", "two", "three", "four"],
        }
    )
    holdout = pa.table(
        {
            "__sts_row_id": pa.array([4, 5, 6, 7], type=pa.int64()),
            "number": pa.array([0, 1, 2, 3], type=pa.int64()),
            "category": ["a", "a", "b", "b"],
            "constant_same": ["x"] * 4,
            "constant_different": ["x"] * 4,
            "all_null": pa.array([None] * 4, type=pa.float64()),
            "record_id": ids,
            "free_text": ["one", "two", "three", "four"],
        }
    )
    synthetic = pa.table(
        {
            "number": pa.array([0, 0, 0, None], type=pa.int64()),
            "category": ["a", "a", "a", "b"],
            "constant_same": ["x"] * 4,
            "constant_different": ["y"] * 4,
            "all_null": pa.array([None] * 4, type=pa.float64()),
            "record_id": ["S-1", "S-2", "S-3", "S-4"],
            "free_text": ["alpha", "beta", "gamma", "delta"],
        }
    )
    return train, holdout, synthetic, columns


def test_primary_golden_distances_missingness_constants_and_aggregate() -> None:
    train, holdout, synthetic, columns = primary_fixture()
    result = evaluate_primary(
        train,
        holdout,
        synthetic,
        columns=columns,
        config=EvaluationConfig(master_seed=123, primary_sample_rows=10),
    )
    by_name = {column.name: column for column in result.columns}

    assert by_name["number"].baseline_distance.value == pytest.approx(0.25)
    # The synthetic side holds a single value while the holdout holds four, so the real
    # KS distance is 0.75. A one-sided constant must not be reported as the maximum 1.0.
    assert by_name["number"].synthetic_distance.value == pytest.approx(0.75)
    assert by_name["number"].baseline_excess.value == pytest.approx(0.5)
    assert by_name["number"].baseline_missingness_difference.value == pytest.approx(
        0.25
    )
    assert by_name["number"].synthetic_missingness_difference.value == pytest.approx(
        0.25
    )
    assert by_name["category"].synthetic_distance.value == pytest.approx(0.25)
    assert by_name["constant_same"].synthetic_distance.value == 0
    assert by_name["constant_different"].synthetic_distance.value == 1
    assert not by_name["all_null"].baseline_distance.applicability.applicable
    assert (
        by_name["all_null"].baseline_distance.applicability.reason == "all_null_sample"
    )
    assert by_name["all_null"].synthetic_missingness_difference.value == 0

    assert result.baseline_excess.eligible_columns == (
        "number",
        "category",
        "constant_same",
        "constant_different",
    )
    assert result.baseline_excess.median.value == pytest.approx(0.375)
    assert result.baseline_excess.p95.value == pytest.approx(0.925)
    assert result.baseline_excess.maximum.value == 1
    assert result.universal_score is None


def test_primary_ci_sample_hashes_applicability_and_identifier_text_exclusion() -> None:
    train, holdout, synthetic, columns = primary_fixture()
    config = EvaluationConfig(master_seed=9876, primary_sample_rows=3)
    first = evaluate_primary(train, holdout, synthetic, columns=columns, config=config)
    second = evaluate_primary(train, holdout, synthetic, columns=columns, config=config)

    assert first == second
    assert first.config_sha256 == config.canonical_sha256
    assert all(manifest.selected_rows == 3 for manifest in first.samples.values())
    assert all(len(manifest.sample_sha256) == 64 for manifest in first.samples.values())
    metric = first.columns[0].baseline_excess
    assert metric.bootstrap_repetitions == 500
    assert metric.confidence_interval is not None
    assert metric.sample_hashes == {
        name: manifest.sample_sha256 for name, manifest in first.samples.items()
    }
    assert metric.sample_seeds == {
        name: manifest.seed for name, manifest in first.samples.items()
    }
    assert metric.sample_rows == {name: 3 for name in first.samples}

    by_name = {column.name: column for column in first.columns}
    assert not by_name["record_id"].included_in_fidelity_aggregate
    assert not by_name["record_id"].baseline_excess.applicability.applicable
    assert not by_name["free_text"].included_in_fidelity_aggregate
    assert "record_id" not in first.baseline_excess.eligible_columns
    assert "free_text" not in first.baseline_excess.eligible_columns


def test_categorical_top_100_other_and_omitted_tail_mass() -> None:
    values = [f"c{index:03d}" for index in range(102)]
    train = pa.table({"category": values})
    holdout = pa.table({"category": values})
    synthetic = pa.table({"category": values[:-2] + ["new-a", "new-b"]})
    result = evaluate_primary(
        train,
        holdout,
        synthetic,
        columns=(model_column("category", ColumnKind.CATEGORICAL),),
        config=EvaluationConfig(master_seed=10, primary_sample_rows=200),
    )
    metric = result.columns[0].synthetic_distance

    assert metric.omitted_tail_mass["real_train_eval"] == pytest.approx(2 / 102)
    assert metric.omitted_tail_mass["real_holdout"] == pytest.approx(2 / 102)
    assert metric.omitted_tail_mass["synthetic"] == pytest.approx(2 / 102)
    assert metric.value == pytest.approx(0)


def test_dp_release_requires_public_categories_and_bins() -> None:
    train = pa.table({"number": [0, 1], "category": ["a", "b"]})
    with pytest.raises(DomainError) as caught:
        evaluate_primary(
            train,
            train,
            train,
            columns=(
                model_column("number", ColumnKind.INTEGER),
                model_column("category", ColumnKind.CATEGORICAL),
            ),
            config=EvaluationConfig(master_seed=1),
            grouping_scope="dp_release",
        )
    assert caught.value.code is ErrorCode.DP_METADATA_NOT_PUBLIC

    result = evaluate_primary(
        train,
        train,
        train,
        columns=(
            model_column("number", ColumnKind.INTEGER, public_bins=(0, 1, 2)),
            model_column(
                "category", ColumnKind.CATEGORICAL, public_categories=("a", "b")
            ),
        ),
        config=EvaluationConfig(master_seed=1),
        grouping_scope="dp_release",
    )
    assert result.grouping_scope == "dp_release"


def test_hmac_sample_is_batch_boundary_independent() -> None:
    table = pa.table(
        {
            "__sts_row_id": pa.array(range(20), type=pa.int64()),
            "value": pa.array(range(20), type=pa.int64()),
        }
    )
    whole, whole_manifest = deterministic_hmac_sample(
        table, max_rows=7, seed=42, namespace="fixture"
    )
    chunked, chunked_manifest = deterministic_hmac_sample(
        table.to_batches(max_chunksize=3), max_rows=7, seed=42, namespace="fixture"
    )

    assert whole.equals(chunked)
    assert whole_manifest == chunked_manifest
    assert whole_manifest.population_rows == 20
    assert whole_manifest.selected_rows == 7


def test_exact_full_scan_counts_hashes_format_and_failures(tmp_path: Path) -> None:
    columns = (
        identifier_column("record_id", pattern=r"ID-\d{3}"),
        model_column("category", ColumnKind.CATEGORICAL, nullable=True),
        model_column("number", ColumnKind.INTEGER),
    )
    table = pa.table(
        {
            "record_id": ["ID-001", "ID-002", "ID-003"],
            "category": pa.array(["a", "a", None], type=pa.string()),
            "number": pa.array([1, 2, 3], type=pa.int64()),
        }
    )
    path = tmp_path / "synthetic.parquet"
    pq.write_table(table, path)
    result = exact_full_scan(path, expected_columns=columns, requested_rows=3)

    checks = {check.name: check for check in result.columns}
    assert result.actual_rows == 3
    assert result.column_order == ("record_id", "category", "number")
    assert checks["category"].null_count == 1
    assert [
        (count.value, count.count) for count in checks["category"].category_counts
    ] == [("a", 2)]
    assert checks["record_id"].unique_non_null_count == 3
    assert checks["record_id"].duplicate_non_null_count == 0
    assert checks["record_id"].format_violations == 0
    assert len(result.artifacts[0].sha256) == 64
    assert len(result.canonical_content_sha256) == 64
    assert (
        exact_full_scan(
            path,
            expected_columns=columns,
            requested_rows=3,
            expected_artifact_sha256=result.artifacts[0].sha256,
            expected_content_sha256=result.canonical_content_sha256,
        ).canonical_content_sha256
        == result.canonical_content_sha256
    )

    with pytest.raises(DomainError) as wrong_rows:
        exact_full_scan(path, expected_columns=columns, requested_rows=4)
    assert wrong_rows.value.code is ErrorCode.OUTPUT_INVALID

    with pytest.raises(DomainError) as wrong_order:
        exact_full_scan(
            path, expected_columns=tuple(reversed(columns)), requested_rows=3
        )
    assert wrong_order.value.code is ErrorCode.OUTPUT_INVALID

    with pytest.raises(DomainError) as wrong_dtype:
        exact_full_scan(
            path,
            expected_columns=columns,
            requested_rows=3,
            expected_arrow_dtypes={"number": "string"},
        )
    assert wrong_dtype.value.code is ErrorCode.OUTPUT_INVALID

    duplicate_path = tmp_path / "duplicates.parquet"
    pq.write_table(
        table.set_column(0, "record_id", pa.array(["ID-001"] * 3)), duplicate_path
    )
    with pytest.raises(DomainError) as duplicate:
        exact_full_scan(duplicate_path, expected_columns=columns, requested_rows=3)
    assert duplicate.value.code is ErrorCode.OUTPUT_INVALID

    with pytest.raises(DomainError) as checksum:
        exact_full_scan(
            path,
            expected_columns=columns,
            requested_rows=3,
            expected_content_sha256="0" * 64,
        )
    assert checksum.value.code is ErrorCode.CHECKSUM_MISMATCH
