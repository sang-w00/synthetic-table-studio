from __future__ import annotations

from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest
from pydantic import ValidationError

from sts.domain import (
    ColumnKind,
    ColumnRole,
    ColumnSchema,
    DomainError,
    ErrorCode,
    IdentifierStrategy,
)
from sts.rules import (
    AllowedValuesRule,
    CompareRule,
    ComparisonPredicate,
    ConditionalSetRule,
    FixedCombinationRule,
    IsNullPredicate,
    MaskPrefixRule,
    NotNullRule,
    RangeRule,
    ReportDenominatorKind,
    RuleProvenance,
    RuleSpec,
    SourceAction,
    SumEqualsRule,
    compile_rules,
)

PUBLIC = RuleProvenance.PUBLIC
PRIVATE = RuleProvenance.PRIVATE_INFERRED


def column(
    name: str,
    kind: ColumnKind,
    *,
    nullable: bool = False,
    decimal_places: int | None = None,
    public_min: int | float | Decimal | str | None = None,
    public_max: int | float | Decimal | str | None = None,
    public_categories: tuple[str | int | bool, ...] | None = None,
) -> ColumnSchema:
    role = ColumnRole.MODEL
    identifier_strategy = None
    if kind is ColumnKind.IDENTIFIER:
        role = ColumnRole.IDENTIFIER
        identifier_strategy = IdentifierStrategy.SEQUENTIAL
    elif kind is ColumnKind.EXCLUDED:
        role = ColumnRole.EXCLUDED
    return ColumnSchema(
        name=name,
        kind=kind,
        nullable=nullable,
        role=role,
        decimal_places=decimal_places,
        public_min=public_min,
        public_max=public_max,
        public_categories=public_categories,
        identifier_strategy=identifier_strategy,
    )


def assert_domain_error(
    code: ErrorCode,
    columns: list[ColumnSchema],
    rules: list[object],
    *,
    mode: str = "utility",
) -> DomainError:
    with pytest.raises(DomainError) as caught:
        compile_rules(columns, rules, mode=mode)  # type: ignore[arg-type]
    assert caught.value.code is code
    return caught.value


def test_rule_spec_is_discriminated_for_exactly_eight_rule_kinds() -> None:
    payloads = [
        {
            "id": "mask",
            "kind": "mask_prefix",
            "provenance": "public",
            "source_action": "block",
            "column": "text",
            "keep_chars": 2,
        },
        {
            "id": "nn",
            "kind": "not_null",
            "provenance": "public",
            "source_action": "drop_row",
            "column": "n",
        },
        {
            "id": "allowed",
            "kind": "allowed_values",
            "provenance": "public",
            "source_action": "block",
            "column": "cat",
            "values": ["a", "b"],
        },
        {
            "id": "range",
            "kind": "range",
            "provenance": "public",
            "source_action": "block",
            "column": "n",
            "min": 0,
            "max": 10,
        },
        {
            "id": "fixed",
            "kind": "fixed_combination",
            "provenance": "public",
            "source_action": "block",
            "columns": ["cat", "flag"],
            "allowed_tuples": [["a", True]],
        },
        {
            "id": "conditional",
            "kind": "conditional_set",
            "provenance": "public",
            "source_action": "block",
            "when": {"kind": "is_null", "column": "cat"},
            "target": "n",
            "value": 0,
        },
        {
            "id": "sum",
            "kind": "sum_equals",
            "provenance": "public",
            "source_action": "block",
            "sources": ["a", "b"],
            "target": "total",
            "tolerance": "0.01",
        },
        {
            "id": "compare",
            "kind": "compare",
            "provenance": "public",
            "source_action": "block",
            "left": "a",
            "op": "<=",
            "right": "b",
        },
    ]
    parsed = [RuleSpec.model_validate(payload).value for payload in payloads]
    assert [rule.kind for rule in parsed] == [
        "mask_prefix",
        "not_null",
        "allowed_values",
        "range",
        "fixed_combination",
        "conditional_set",
        "sum_equals",
        "compare",
    ]

    bad = dict(payloads[0], kind="unknown")
    with pytest.raises(ValidationError):
        RuleSpec.model_validate(bad)


def test_predicates_are_discriminated_and_null_requires_is_null() -> None:
    comparison = RuleSpec.model_validate(
        {
            "id": "set",
            "kind": "conditional_set",
            "provenance": "public",
            "source_action": "block",
            "when": {"kind": "comparison", "column": "cat", "op": "eq", "value": "a"},
            "target": "n",
            "value": 1,
        }
    ).value
    assert isinstance(comparison, ConditionalSetRule)
    assert isinstance(comparison.when, ComparisonPredicate)

    null_check = ConditionalSetRule(
        id="null-check",
        provenance=PUBLIC,
        when=IsNullPredicate(column="cat"),
        target="n",
        value=0,
    )
    assert isinstance(null_check.when, IsNullPredicate)

    with pytest.raises(ValidationError):
        RuleSpec.model_validate(
            {
                "id": "bad-null",
                "kind": "conditional_set",
                "provenance": "public",
                "source_action": "block",
                "when": {
                    "kind": "comparison",
                    "column": "cat",
                    "op": "eq",
                    "value": None,
                },
                "target": "n",
                "value": 1,
            }
        )


def test_source_action_constraints_and_frozen_specs() -> None:
    with pytest.raises(ValidationError):
        MaskPrefixRule(
            id="mask",
            provenance=PUBLIC,
            source_action=SourceAction.DROP_ROW,
            column="text",
            keep_chars=1,
        )

    validation_rules = [
        NotNullRule(id="nn", provenance=PUBLIC, source_action="drop_row", column="n"),
        AllowedValuesRule(
            id="allowed",
            provenance=PUBLIC,
            source_action="drop_row",
            column="cat",
            values=("a",),
        ),
        RangeRule(
            id="range",
            provenance=PUBLIC,
            source_action="drop_row",
            column="n",
            min=0,
            max=1,
        ),
        FixedCombinationRule(
            id="fixed",
            provenance=PUBLIC,
            source_action="drop_row",
            columns=("a", "b"),
            allowed_tuples=(("x", "y"),),
        ),
        ConditionalSetRule(
            id="conditional",
            provenance=PUBLIC,
            source_action="drop_row",
            when=IsNullPredicate(column="cat"),
            target="n",
            value=0,
        ),
        SumEqualsRule(
            id="sum",
            provenance=PUBLIC,
            source_action="drop_row",
            sources=("a",),
            target="n",
        ),
        CompareRule(
            id="compare",
            provenance=PUBLIC,
            source_action="drop_row",
            left="a",
            op="<=",
            right="b",
        ),
    ]
    assert all(rule.source_action == SourceAction.DROP_ROW for rule in validation_rules)
    with pytest.raises(ValidationError):
        validation_rules[0].column = "other"  # type: ignore[misc]


def test_each_rule_compiles_for_its_valid_type_and_domain() -> None:
    cases = [
        (
            [column("text", ColumnKind.TEXT, nullable=True)],
            MaskPrefixRule(id="mask", provenance=PUBLIC, column="text", keep_chars=2),
        ),
        (
            [column("ignored", ColumnKind.EXCLUDED, nullable=True)],
            NotNullRule(id="nn", provenance=PUBLIC, column="ignored"),
        ),
        (
            [column("flag", ColumnKind.BOOLEAN, nullable=True)],
            AllowedValuesRule(
                id="allowed", provenance=PUBLIC, column="flag", values=(True,)
            ),
        ),
        (
            [column("day", ColumnKind.DATE, nullable=True)],
            RangeRule(
                id="range",
                provenance=PUBLIC,
                column="day",
                min="2025-01-01",
                max="2025-12-31",
            ),
        ),
        (
            [
                column("cat", ColumnKind.CATEGORICAL, nullable=True),
                column("flag", ColumnKind.BOOLEAN),
            ],
            FixedCombinationRule(
                id="fixed",
                provenance=PUBLIC,
                columns=("cat", "flag"),
                allowed_tuples=((None, False), ("a", True)),
            ),
        ),
        (
            [column("cat", ColumnKind.CATEGORICAL), column("n", ColumnKind.INTEGER)],
            ConditionalSetRule(
                id="conditional",
                provenance=PUBLIC,
                when=ComparisonPredicate(column="cat", op="eq", value="a"),
                target="n",
                value=1,
            ),
        ),
        (
            [
                column("a", ColumnKind.INTEGER, public_min=0, public_max=10),
                column("b", ColumnKind.INTEGER, public_min=0, public_max=20),
                column("total", ColumnKind.INTEGER, public_min=0, public_max=30),
            ],
            SumEqualsRule(
                id="sum",
                provenance=PUBLIC,
                sources=("a", "b"),
                target="total",
                tolerance=Decimal("0.5"),
            ),
        ),
        (
            [column("start", ColumnKind.DATE), column("end", ColumnKind.DATE)],
            CompareRule(
                id="compare", provenance=PUBLIC, left="start", op="<", right="end"
            ),
        ),
    ]
    for columns, rule in cases:
        compiled = compile_rules(columns, [rule])
        assert compiled.rules == (rule,)
        assert compiled.node_by_id[rule.id].spec is rule


@pytest.mark.parametrize("kind", list(ColumnKind))
def test_not_null_accepts_every_column_type(kind: ColumnKind) -> None:
    kwargs = {"decimal_places": 2} if kind is ColumnKind.FIXED_DECIMAL else {}
    schema = column("value", kind, nullable=True, **kwargs)
    rule = NotNullRule(id="required", provenance=PUBLIC, column="value")
    assert compile_rules([schema], [rule]).rules == (rule,)


def test_nullable_rule_semantics_compile_without_coercing_null_into_domains() -> None:
    nullable_int = column("n", ColumnKind.INTEGER, nullable=True)
    assert compile_rules(
        [nullable_int],
        [AllowedValuesRule(id="allowed", provenance=PUBLIC, column="n", values=(1, 2))],
    )
    assert compile_rules(
        [nullable_int],
        [RangeRule(id="range", provenance=PUBLIC, column="n", min=0, max=2)],
    )
    assert compile_rules(
        [nullable_int, column("total", ColumnKind.INTEGER)],
        [SumEqualsRule(id="sum", provenance=PUBLIC, sources=("n",), target="total")],
    )
    assert compile_rules(
        [nullable_int, column("m", ColumnKind.INTEGER, nullable=True)],
        [CompareRule(id="compare", provenance=PUBLIC, left="n", op="<=", right="m")],
    )

    nullable_target = column("target", ColumnKind.TEXT, nullable=True)
    rule = ConditionalSetRule(
        id="clear",
        provenance=PUBLIC,
        when=IsNullPredicate(column="target"),
        target="target",
        value=None,
    )
    assert compile_rules([nullable_target], [rule]).rules == (rule,)


def test_null_in_tuple_and_conditional_value_requires_nullable_schema() -> None:
    fixed = FixedCombinationRule(
        id="fixed",
        provenance=PUBLIC,
        columns=("a", "b"),
        allowed_tuples=((None, True),),
    )
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        [column("a", ColumnKind.CATEGORICAL), column("b", ColumnKind.BOOLEAN)],
        [fixed],
    )

    conditional = ConditionalSetRule(
        id="clear",
        provenance=PUBLIC,
        when=IsNullPredicate(column="target"),
        target="target",
        value=None,
    )
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        [column("target", ColumnKind.TEXT)],
        [conditional],
    )


@pytest.mark.parametrize(
    ("columns", "rule"),
    [
        (
            [column("n", ColumnKind.INTEGER)],
            MaskPrefixRule(id="mask", provenance=PUBLIC, column="n", keep_chars=1),
        ),
        (
            [column("cat", ColumnKind.CATEGORICAL)],
            RangeRule(id="range", provenance=PUBLIC, column="cat", min="a", max="z"),
        ),
        (
            [column("n", ColumnKind.INTEGER), column("flag", ColumnKind.BOOLEAN)],
            FixedCombinationRule(
                id="fixed",
                provenance=PUBLIC,
                columns=("n", "flag"),
                allowed_tuples=((1, True),),
            ),
        ),
        (
            [column("cat", ColumnKind.CATEGORICAL), column("n", ColumnKind.INTEGER)],
            ConditionalSetRule(
                id="conditional",
                provenance=PUBLIC,
                when=ComparisonPredicate(column="cat", op="lt", value="z"),
                target="n",
                value=1,
            ),
        ),
        (
            [column("a", ColumnKind.FLOAT), column("total", ColumnKind.FLOAT)],
            SumEqualsRule(id="sum", provenance=PUBLIC, sources=("a",), target="total"),
        ),
        (
            [column("a", ColumnKind.INTEGER), column("b", ColumnKind.DATE)],
            CompareRule(id="compare", provenance=PUBLIC, left="a", op="<=", right="b"),
        ),
        (
            [
                column("a", ColumnKind.FIXED_DECIMAL, decimal_places=2),
                column("b", ColumnKind.FIXED_DECIMAL, decimal_places=3),
            ],
            CompareRule(id="compare", provenance=PUBLIC, left="a", op="<=", right="b"),
        ),
    ],
)
def test_rule_type_constraints_reject_invalid_columns(
    columns: list[ColumnSchema], rule: object
) -> None:
    assert_domain_error(ErrorCode.RULE_CONFLICT, columns, [rule])


def test_scalar_and_domain_validation_rejects_mismatches_and_empty_ranges() -> None:
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        [column("n", ColumnKind.INTEGER)],
        [AllowedValuesRule(id="allowed", provenance=PUBLIC, column="n", values=("1",))],
    )
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        [column("n", ColumnKind.INTEGER)],
        [RangeRule(id="range", provenance=PUBLIC, column="n", min=2, max=1)],
    )
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        [column("n", ColumnKind.INTEGER)],
        [
            RangeRule(
                id="range",
                provenance=PUBLIC,
                column="n",
                min=1,
                max=1,
                inclusive_min=False,
            )
        ],
    )
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        [column("f", ColumnKind.FLOAT)],
        [
            AllowedValuesRule(
                id="allowed",
                provenance=PUBLIC,
                column="f",
                values=(1, Decimal("1")),
            )
        ],
    )
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        [column("d", ColumnKind.DATE)],
        [
            AllowedValuesRule(
                id="allowed", provenance=PUBLIC, column="d", values=("not-a-date",)
            )
        ],
    )
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        [column("flag", ColumnKind.BOOLEAN), column("n", ColumnKind.INTEGER)],
        [
            ConditionalSetRule(
                id="set",
                provenance=PUBLIC,
                when=ComparisonPredicate(column="flag", op="eq", value=True),
                target="n",
                value="one",
            )
        ],
    )


def test_model_validation_rejects_invalid_rule_local_domains() -> None:
    with pytest.raises(ValidationError):
        AllowedValuesRule(id="allowed", provenance=PUBLIC, column="n", values=())
    with pytest.raises(ValidationError):
        AllowedValuesRule(id="allowed", provenance=PUBLIC, column="n", values=(1, 1))
    with pytest.raises(ValidationError):
        FixedCombinationRule(
            id="fixed",
            provenance=PUBLIC,
            columns=("a", "b"),
            allowed_tuples=(("a",),),
        )
    with pytest.raises(ValidationError):
        FixedCombinationRule(
            id="fixed",
            provenance=PUBLIC,
            columns=("a", "a"),
            allowed_tuples=(("a", "a"),),
        )
    with pytest.raises(ValidationError):
        SumEqualsRule(
            id="sum",
            provenance=PUBLIC,
            sources=("a", "a"),
            target="total",
        )
    with pytest.raises(ValidationError):
        SumEqualsRule(
            id="sum",
            provenance=PUBLIC,
            sources=("a",),
            target="total",
            tolerance=Decimal("NaN"),
        )


def test_utility_source_inferred_fixed_tuples_are_explicitly_private() -> None:
    columns = [
        column("a", ColumnKind.CATEGORICAL),
        column("b", ColumnKind.BOOLEAN),
    ]
    inferred = FixedCombinationRule(
        id="fixed",
        provenance=PRIVATE,
        columns=("a", "b"),
    )
    assert compile_rules(columns, [inferred]).rules == (inferred,)

    falsely_public = FixedCombinationRule(
        id="fixed",
        provenance=PUBLIC,
        columns=("a", "b"),
    )
    assert_domain_error(ErrorCode.RULE_CONFLICT, columns, [falsely_public])


def test_duplicate_ids_unknown_columns_and_multiple_writers_conflict() -> None:
    schema = [
        column("text", ColumnKind.TEXT),
        column("flag", ColumnKind.BOOLEAN),
    ]
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        schema,
        [
            NotNullRule(id="duplicate", provenance=PUBLIC, column="text"),
            NotNullRule(id="duplicate", provenance=PUBLIC, column="flag"),
        ],
    )
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        schema,
        [NotNullRule(id="missing", provenance=PUBLIC, column="absent")],
    )
    error = assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        schema,
        [
            MaskPrefixRule(id="mask", provenance=PUBLIC, column="text", keep_chars=1),
            ConditionalSetRule(
                id="set",
                provenance=PUBLIC,
                when=ComparisonPredicate(column="flag", op="eq", value=True),
                target="text",
                value="constant",
            ),
        ],
    )
    assert error.problem.context["column"] == "text"


def test_structural_transform_overlap_is_rejected() -> None:
    schema = [
        column("a", ColumnKind.INTEGER),
        column("b", ColumnKind.INTEGER),
        column("c", ColumnKind.INTEGER),
    ]
    error = assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        schema,
        [
            CompareRule(id="ab", provenance=PUBLIC, left="a", op="<=", right="b"),
            CompareRule(id="bc", provenance=PUBLIC, left="b", op="<=", right="c"),
        ],
    )
    assert "structural" in error.problem.detail


def test_condition_read_after_another_writer_is_ambiguous() -> None:
    schema = [
        column("flag", ColumnKind.BOOLEAN),
        column("a", ColumnKind.INTEGER),
        column("b", ColumnKind.INTEGER),
        column("total", ColumnKind.INTEGER),
    ]
    error = assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        schema,
        [
            SumEqualsRule(
                id="sum", provenance=PUBLIC, sources=("a", "b"), target="total"
            ),
            ConditionalSetRule(
                id="conditional",
                provenance=PUBLIC,
                when=ComparisonPredicate(column="total", op="gt", value=0),
                target="flag",
                value=True,
            ),
        ],
    )
    assert "ambiguous" in error.problem.detail


def test_sum_target_source_overlap_is_rejected() -> None:
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        [column("a", ColumnKind.INTEGER), column("b", ColumnKind.INTEGER)],
        [SumEqualsRule(id="sum", provenance=PUBLIC, sources=("a", "b"), target="a")],
    )


def test_reconstruction_cycles_are_rejected() -> None:
    schema = [column("a", ColumnKind.INTEGER), column("b", ColumnKind.INTEGER)]
    error = assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        schema,
        [
            SumEqualsRule(id="to-a", provenance=PUBLIC, sources=("b",), target="a"),
            SumEqualsRule(id="to-b", provenance=PUBLIC, sources=("a",), target="b"),
        ],
    )
    assert "cycle" in error.problem.detail


def test_reconstruction_order_uses_dependencies_and_not_ui_order() -> None:
    schema = [
        column("flag", ColumnKind.BOOLEAN),
        column("part", ColumnKind.INTEGER),
        column("subtotal", ColumnKind.INTEGER),
        column("total", ColumnKind.INTEGER),
        column("upper", ColumnKind.INTEGER),
    ]
    rules = [
        CompareRule(
            id="r03-compare", provenance=PUBLIC, left="total", op="<=", right="upper"
        ),
        SumEqualsRule(
            id="r02-sum",
            provenance=PUBLIC,
            sources=("subtotal", "part"),
            target="total",
        ),
        ConditionalSetRule(
            id="r01-set",
            provenance=PUBLIC,
            when=ComparisonPredicate(column="flag", op="eq", value=True),
            target="subtotal",
            value=0,
        ),
    ]
    compiled = compile_rules(schema, rules)
    reversed_compiled = compile_rules(schema, list(reversed(rules)))
    expected = ("r01-set", "r02-sum", "r03-compare")
    assert compiled.reconstruction_order == expected
    assert reversed_compiled.reconstruction_order == expected
    assert tuple(rule.id for rule in compiled.reconstruction_phase) == expected
    assert [
        (edge.before_rule_id, edge.after_rule_id) for edge in compiled.dependencies
    ] == [
        ("r01-set", "r02-sum"),
        ("r02-sum", "r03-compare"),
    ]


def test_independent_reconstructors_have_canonical_confluent_order() -> None:
    schema = [
        column("f1", ColumnKind.BOOLEAN),
        column("f2", ColumnKind.BOOLEAN),
        column("a", ColumnKind.INTEGER),
        column("b", ColumnKind.INTEGER),
    ]
    later = ConditionalSetRule(
        id="z-rule",
        provenance=PUBLIC,
        when=ComparisonPredicate(column="f1", op="eq", value=True),
        target="a",
        value=1,
    )
    earlier = ConditionalSetRule(
        id="a-rule",
        provenance=PUBLIC,
        when=ComparisonPredicate(column="f2", op="eq", value=True),
        target="b",
        value=2,
    )
    assert compile_rules(schema, [later, earlier]).reconstruction_order == (
        "a-rule",
        "z-rule",
    )


def test_compiled_plan_exposes_immutable_phases_graph_schema_and_denominators() -> None:
    schema = [
        column("text", ColumnKind.TEXT, nullable=True),
        column("flag", ColumnKind.BOOLEAN),
        column("n", ColumnKind.INTEGER),
    ]
    mask = MaskPrefixRule(id="mask", provenance=PUBLIC, column="text", keep_chars=1)
    required = NotNullRule(id="required", provenance=PUBLIC, column="n")
    conditional = ConditionalSetRule(
        id="set",
        provenance=PUBLIC,
        when=ComparisonPredicate(column="flag", op="eq", value=True),
        target="n",
        value=0,
    )
    compiled = compile_rules(schema, [conditional, required, mask])

    assert tuple(rule.id for rule in compiled.rules) == ("mask", "required", "set")
    assert tuple(rule.id for rule in compiled.source_phase) == ("required", "set")
    assert tuple(rule.id for rule in compiled.model_phase) == ("mask", "required")
    assert compiled.reconstruction_order == ("set",)
    denominators = {item.rule_id: item for item in compiled.report_denominators}
    assert denominators["mask"].kind is ReportDenominatorKind.NON_NULL_ROWS
    assert denominators["required"].kind is ReportDenominatorKind.ALL_ROWS
    assert denominators["set"].kind is ReportDenominatorKind.CONDITION_TRUE_ROWS
    assert compiled.writer_by_column == {"text": "mask", "n": "set"}
    assert compiled.schema_by_name["text"].nullable is True

    with pytest.raises(TypeError):
        compiled.schema_by_name["new"] = column("new", ColumnKind.INTEGER)  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        compiled.mode = "differential_privacy"  # type: ignore[misc]


def private_rules_for_dp_gate() -> list[tuple[list[ColumnSchema], object]]:
    return [
        (
            [column("text", ColumnKind.TEXT)],
            MaskPrefixRule(id="mask", provenance=PRIVATE, column="text", keep_chars=1),
        ),
        (
            [column("n", ColumnKind.INTEGER)],
            NotNullRule(id="nn", provenance=PRIVATE, column="n"),
        ),
        (
            [column("n", ColumnKind.INTEGER)],
            AllowedValuesRule(
                id="allowed", provenance=PRIVATE, column="n", values=(1,)
            ),
        ),
        (
            [column("n", ColumnKind.INTEGER)],
            RangeRule(id="range", provenance=PRIVATE, column="n", min=0, max=1),
        ),
        (
            [column("a", ColumnKind.CATEGORICAL), column("b", ColumnKind.BOOLEAN)],
            FixedCombinationRule(id="fixed", provenance=PRIVATE, columns=("a", "b")),
        ),
        (
            [column("flag", ColumnKind.BOOLEAN), column("n", ColumnKind.INTEGER)],
            ConditionalSetRule(
                id="conditional",
                provenance=PRIVATE,
                when=ComparisonPredicate(column="flag", op="eq", value=True),
                target="n",
                value=1,
            ),
        ),
        (
            [column("a", ColumnKind.INTEGER), column("total", ColumnKind.INTEGER)],
            SumEqualsRule(id="sum", provenance=PRIVATE, sources=("a",), target="total"),
        ),
        (
            [column("a", ColumnKind.INTEGER), column("b", ColumnKind.INTEGER)],
            CompareRule(id="compare", provenance=PRIVATE, left="a", op="<=", right="b"),
        ),
    ]


@pytest.mark.parametrize(("columns", "rule"), private_rules_for_dp_gate())
def test_dp_rejects_private_inferred_provenance_for_every_rule(
    columns: list[ColumnSchema], rule: object
) -> None:
    assert_domain_error(
        ErrorCode.DP_METADATA_NOT_PUBLIC,
        columns,
        [rule],
        mode="differential_privacy",
    )


def test_dp_requires_public_fixed_tuples_and_accepts_explicit_public_tuples() -> None:
    schema = [
        column("a", ColumnKind.CATEGORICAL, nullable=True),
        column("b", ColumnKind.BOOLEAN),
    ]
    missing = FixedCombinationRule(id="fixed", provenance=PUBLIC, columns=("a", "b"))
    assert_domain_error(
        ErrorCode.DP_METADATA_NOT_PUBLIC,
        schema,
        [missing],
        mode="differential_privacy",
    )

    explicit = FixedCombinationRule(
        id="fixed",
        provenance=PUBLIC,
        columns=("a", "b"),
        allowed_tuples=((None, False), ("x", True)),
    )
    assert compile_rules(schema, [explicit], mode="differential_privacy").rules == (
        explicit,
    )


def test_dp_public_rule_domain_must_fit_public_category_domain() -> None:
    schema = [
        column(
            "cat",
            ColumnKind.CATEGORICAL,
            public_categories=("a", "b"),
        )
    ]
    rule = AllowedValuesRule(
        id="allowed",
        provenance=PUBLIC,
        column="cat",
        values=("a", "private-value"),
    )
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        schema,
        [rule],
        mode="differential_privacy",
    )


def test_dp_accepts_public_range_bounds() -> None:
    rule = RangeRule(id="range", provenance=PUBLIC, column="n", min=0, max=10)
    compiled = compile_rules(
        [column("n", ColumnKind.INTEGER)],
        [rule],
        mode="differential_privacy",
    )
    assert compiled.rules == (rule,)


def test_float_compare_requires_positive_public_granularity() -> None:
    schema = [column("a", ColumnKind.FLOAT), column("b", ColumnKind.FLOAT)]
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        schema,
        [CompareRule(id="compare", provenance=PUBLIC, left="a", op="<", right="b")],
    )
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        schema,
        [
            CompareRule(
                id="compare",
                provenance=PRIVATE,
                left="a",
                op="<",
                right="b",
                granularity=0.01,
            )
        ],
    )
    public = CompareRule(
        id="compare",
        provenance=PUBLIC,
        left="a",
        op="<",
        right="b",
        granularity=Decimal("0.01"),
    )
    assert compile_rules(schema, [public], mode="differential_privacy").rules == (
        public,
    )

    with pytest.raises(ValidationError):
        CompareRule(
            id="compare",
            provenance=PUBLIC,
            left="a",
            op="<",
            right="b",
            granularity=0,
        )


def test_non_float_compare_uses_public_type_unit_not_custom_granularity() -> None:
    assert_domain_error(
        ErrorCode.RULE_CONFLICT,
        [column("a", ColumnKind.INTEGER), column("b", ColumnKind.INTEGER)],
        [
            CompareRule(
                id="compare",
                provenance=PUBLIC,
                left="a",
                op="<",
                right="b",
                granularity=1,
            )
        ],
    )


def test_sum_static_int64_overflow_and_target_domain_are_rejected() -> None:
    overflow_schema = [
        column(
            "a", ColumnKind.INTEGER, public_min=0, public_max=5_000_000_000_000_000_000
        ),
        column(
            "b", ColumnKind.INTEGER, public_min=0, public_max=5_000_000_000_000_000_000
        ),
        column("total", ColumnKind.INTEGER),
    ]
    rule = SumEqualsRule(
        id="sum", provenance=PUBLIC, sources=("a", "b"), target="total"
    )
    assert_domain_error(ErrorCode.RULE_CONFLICT, overflow_schema, [rule])

    narrow_target_schema = [
        column("a", ColumnKind.INTEGER, public_min=0, public_max=5),
        column("b", ColumnKind.INTEGER, public_min=0, public_max=5),
        column("total", ColumnKind.INTEGER, public_min=0, public_max=9),
    ]
    assert_domain_error(ErrorCode.RULE_CONFLICT, narrow_target_schema, [rule])


def test_sum_marks_missing_bounds_for_runtime_overflow_check() -> None:
    schema = [
        column("a", ColumnKind.INTEGER, public_min=0, public_max=5),
        column("b", ColumnKind.INTEGER),
        column("total", ColumnKind.INTEGER),
    ]
    rule = SumEqualsRule(
        id="sum", provenance=PUBLIC, sources=("a", "b"), target="total"
    )
    compiled = compile_rules(schema, [rule])
    assert compiled.node_by_id["sum"].requires_runtime_overflow_check is True


def test_sum_fixed_decimal_scaled_bounds_must_fit_int64() -> None:
    schema = [
        column(
            "amount",
            ColumnKind.FIXED_DECIMAL,
            decimal_places=18,
            public_min=Decimal("0"),
            public_max=Decimal("10"),
        ),
        column("total", ColumnKind.FIXED_DECIMAL, decimal_places=18),
    ]
    rule = SumEqualsRule(
        id="sum", provenance=PUBLIC, sources=("amount",), target="total"
    )
    assert_domain_error(ErrorCode.RULE_CONFLICT, schema, [rule])
