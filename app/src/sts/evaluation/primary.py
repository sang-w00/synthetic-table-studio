from __future__ import annotations

import math
import random
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal

import pyarrow as pa
from pydantic import Field

from sts.domain.canonical import CanonicalModel, canonical_json_bytes
from sts.domain.errors import DomainError, ErrorCode
from sts.domain.models import ColumnKind, ColumnSchema

from .config import EvaluationConfig
from .sampling import SampleManifest, deterministic_hmac_sample


class Applicability(CanonicalModel):
    applicable: bool
    reason: str | None = None


class ConfidenceInterval(CanonicalModel):
    level: Literal[0.95] = 0.95
    method: Literal["percentile_bootstrap"] = "percentile_bootstrap"
    lower: float
    upper: float


class MetricEstimate(CanonicalModel):
    metric: str
    applicability: Applicability
    value: float | None
    confidence_interval: ConfidenceInterval | None
    sample_hashes: Mapping[str, str]
    sample_seeds: Mapping[str, int]
    sample_rows: Mapping[str, int]
    omitted_tail_mass: Mapping[str, float]
    bootstrap_seed: int = Field(ge=0, le=2**64 - 1)
    bootstrap_repetitions: Literal[500] = 500


class ColumnPrimaryResult(CanonicalModel):
    name: str
    kind: ColumnKind
    included_in_fidelity_aggregate: bool
    baseline_distance: MetricEstimate
    synthetic_distance: MetricEstimate
    baseline_excess: MetricEstimate
    baseline_missingness_difference: MetricEstimate
    synthetic_missingness_difference: MetricEstimate


class BaselineExcessAggregate(CanonicalModel):
    eligible_columns: tuple[str, ...]
    median: MetricEstimate
    p95: MetricEstimate
    maximum: MetricEstimate


class PrimaryEvaluationResult(CanonicalModel):
    version: Literal["1.0"] = "1.0"
    config_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    grouping_scope: Literal["utility_internal", "dp_release"]
    samples: Mapping[str, SampleManifest]
    columns: tuple[ColumnPrimaryResult, ...]
    baseline_excess: BaselineExcessAggregate
    universal_score: None = None


_PRIMARY_KINDS = {
    ColumnKind.INTEGER,
    ColumnKind.FIXED_DECIMAL,
    ColumnKind.FLOAT,
    ColumnKind.DATE,
    ColumnKind.DATETIME,
    ColumnKind.CATEGORICAL,
    ColumnKind.BOOLEAN,
}
_NUMERIC_KINDS = {
    ColumnKind.INTEGER,
    ColumnKind.FIXED_DECIMAL,
    ColumnKind.FLOAT,
    ColumnKind.DATE,
    ColumnKind.DATETIME,
}


def _quantile(values: Sequence[float], probability: float) -> float:
    if not values:
        raise ValueError("a quantile requires at least one value")
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return float(ordered[lower])
    weight = position - lower
    return float(ordered[lower] * (1 - weight) + ordered[upper] * weight)


def _ci(values: Sequence[float]) -> ConfidenceInterval | None:
    if not values:
        return None
    return ConfidenceInterval(lower=_quantile(values, 0.025), upper=_quantile(values, 0.975))


def _ordered_numeric(value: object, kind: ColumnKind) -> int | float | Decimal:
    if kind is ColumnKind.DATE:
        if not isinstance(value, date) or isinstance(value, datetime):
            raise ValueError("date metric received a non-date value")
        return (value - date(1970, 1, 1)).days
    if kind is ColumnKind.DATETIME:
        if not isinstance(value, datetime):
            raise ValueError("datetime metric received a non-datetime value")
        if value.tzinfo is None or value.utcoffset() is None:
            value = value.replace(tzinfo=UTC)
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        delta = value.astimezone(UTC) - epoch
        return delta.days * 86_400_000_000 + delta.seconds * 1_000_000 + delta.microseconds
    if kind is ColumnKind.FLOAT:
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("KS distance does not support non-finite floats")
        return number
    if kind is ColumnKind.FIXED_DECIMAL:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    if isinstance(value, bool):
        raise ValueError("integer metric received a boolean value")
    return int(value)


def _constant_distance(left: Sequence[object], right: Sequence[object]) -> float | None:
    if not left or not right:
        return None
    left_unique = set(left)
    right_unique = set(right)
    # Only a genuinely degenerate pair short-circuits. When one side is constant and
    # the other is not, the real KS/TVD distance is well defined and usually small;
    # returning 1.0 there would make that column dominate every aggregate.
    if len(left_unique) == 1 and len(right_unique) == 1:
        return 0.0 if left_unique == right_unique else 1.0
    return None


def _ks_distance(left: Sequence[object], right: Sequence[object], kind: ColumnKind) -> float | None:
    constant = _constant_distance(left, right)
    if constant is not None:
        return constant
    if not left or not right:
        return None
    left_counts = Counter(_ordered_numeric(value, kind) for value in left)
    right_counts = Counter(_ordered_numeric(value, kind) for value in right)
    left_total = len(left)
    right_total = len(right)
    left_seen = 0
    right_seen = 0
    distance = 0.0
    for value in sorted(left_counts.keys() | right_counts.keys()):
        left_seen += left_counts.get(value, 0)
        right_seen += right_counts.get(value, 0)
        distance = max(distance, abs(left_seen / left_total - right_seen / right_total))
    return distance


def _group(value: object, categories: frozenset[object]) -> object:
    return value if value in categories else "__OTHER__"


def _tvd_distance(
    left: Sequence[object],
    right: Sequence[object],
    categories: frozenset[object],
) -> float | None:
    if not left or not right:
        return None
    grouped_left = [_group(value, categories) for value in left]
    grouped_right = [_group(value, categories) for value in right]
    constant = _constant_distance(grouped_left, grouped_right)
    if constant is not None:
        return constant
    left_counts = Counter(grouped_left)
    right_counts = Counter(grouped_right)
    support = left_counts.keys() | right_counts.keys()
    return 0.5 * sum(
        abs(left_counts.get(value, 0) / len(left) - right_counts.get(value, 0) / len(right))
        for value in support
    )


def _top_categories(values: Sequence[object], limit: int) -> tuple[object, ...]:
    counts = Counter(values)
    ordered = sorted(counts.items(), key=lambda item: (-item[1], canonical_json_bytes(item[0])))
    return tuple(value for value, _ in ordered[:limit])


def _omitted_mass(values: Sequence[object], categories: frozenset[object]) -> float:
    if not values:
        return 0.0
    return sum(value not in categories for value in values) / len(values)


def _bootstrap_samples(values: Sequence[object], rng: random.Random) -> list[object]:
    return rng.choices(values, k=len(values)) if values else []


def _provenance(
    manifests: Mapping[str, SampleManifest],
) -> tuple[dict[str, str], dict[str, int], dict[str, int]]:
    return (
        {name: manifest.sample_sha256 for name, manifest in manifests.items()},
        {name: manifest.seed for name, manifest in manifests.items()},
        {name: manifest.selected_rows for name, manifest in manifests.items()},
    )


def _estimate(
    *,
    metric: str,
    value: float | None,
    bootstrap_values: Sequence[float],
    manifests: Mapping[str, SampleManifest],
    omitted_tail_mass: Mapping[str, float],
    bootstrap_seed: int,
    reason: str | None = None,
) -> MetricEstimate:
    hashes, seeds, rows = _provenance(manifests)
    applicable = value is not None
    return MetricEstimate(
        metric=metric,
        applicability=Applicability(applicable=applicable, reason=None if applicable else reason),
        value=value,
        confidence_interval=_ci(bootstrap_values) if applicable else None,
        sample_hashes=hashes,
        sample_seeds=seeds,
        sample_rows=rows,
        omitted_tail_mass=dict(omitted_tail_mass),
        bootstrap_seed=bootstrap_seed,
    )


def _missing_rate(values: Sequence[object]) -> float:
    return sum(value is None for value in values) / len(values) if values else 0.0


def _excluded_column(
    column: ColumnSchema,
    manifests: Mapping[str, SampleManifest],
    config: EvaluationConfig,
) -> ColumnPrimaryResult:
    reason = "identifier_text_or_excluded_column"
    omitted = {name: 0.0 for name in manifests}

    def unavailable(metric: str) -> MetricEstimate:
        return _estimate(
            metric=metric,
            value=None,
            bootstrap_values=(),
            manifests=manifests,
            omitted_tail_mass=omitted,
            bootstrap_seed=config.derive_seed(f"primary:{column.name}:{metric}"),
            reason=reason,
        )

    return ColumnPrimaryResult(
        name=column.name,
        kind=column.kind,
        included_in_fidelity_aggregate=False,
        baseline_distance=unavailable("not_applicable"),
        synthetic_distance=unavailable("not_applicable"),
        baseline_excess=unavailable("baseline_excess"),
        baseline_missingness_difference=unavailable("missingness_absolute_difference"),
        synthetic_missingness_difference=unavailable("missingness_absolute_difference"),
    )


def _require_public_grouping(columns: Sequence[ColumnSchema]) -> None:
    missing: list[str] = []
    for column in columns:
        if column.kind in {ColumnKind.CATEGORICAL, ColumnKind.BOOLEAN}:
            if column.public_categories is None:
                missing.append(f"{column.name}:public_categories")
        elif column.kind in _NUMERIC_KINDS and column.public_bins is None:
            missing.append(f"{column.name}:public_bins")
    if missing:
        raise DomainError(
            ErrorCode.DP_METADATA_NOT_PUBLIC,
            "DP release evaluation grouping requires public categories and bins",
            context={"missing": missing},
        )


def evaluate_primary(
    real_train_eval: pa.Table,
    real_holdout: pa.Table,
    synthetic: pa.Table,
    *,
    columns: Sequence[ColumnSchema],
    config: EvaluationConfig,
    grouping_scope: Literal["utility_internal", "dp_release"] = "utility_internal",
) -> PrimaryEvaluationResult:
    """Compute version 1.0 primary fidelity without producing an overall score."""

    columns = tuple(columns)
    names = [column.name for column in columns]
    if len(names) != len(set(names)):
        raise ValueError("evaluation columns must have unique names")
    for source_name, table in {
        "real_train_eval": real_train_eval,
        "real_holdout": real_holdout,
        "synthetic": synthetic,
    }.items():
        missing = [name for name in names if name not in table.column_names]
        if missing:
            raise DomainError(
                ErrorCode.OUTPUT_INVALID,
                f"{source_name} is missing evaluation columns",
                context={"missing": missing},
            )
    if grouping_scope == "dp_release":
        _require_public_grouping([column for column in columns if column.kind in _PRIMARY_KINDS])

    source_tables = {
        "real_train_eval": real_train_eval,
        "real_holdout": real_holdout,
        "synthetic": synthetic,
    }
    samples: dict[str, pa.Table] = {}
    manifests: dict[str, SampleManifest] = {}
    for source_name, table in source_tables.items():
        seed = config.derive_seed(f"primary-sample:{source_name}")
        sample, manifest = deterministic_hmac_sample(
            table,
            max_rows=config.primary_sample_rows,
            seed=seed,
            namespace=f"primary-1.0:{source_name}",
            hmac_key=config.derive_hmac_key(f"primary-sample:{source_name}"),
        )
        samples[source_name] = sample
        manifests[source_name] = manifest

    results: list[ColumnPrimaryResult] = []
    eligible_names: list[str] = []
    eligible_excess: list[float] = []
    eligible_bootstraps: list[list[float]] = []
    for column in columns:
        if column.kind not in _PRIMARY_KINDS:
            results.append(_excluded_column(column, manifests, config))
            continue
        raw = {
            name: table.column(column.name).combine_chunks().to_pylist()
            for name, table in samples.items()
        }
        nonnull = {
            name: [value for value in values if value is not None] for name, values in raw.items()
        }
        if column.kind in _NUMERIC_KINDS:
            categories: frozenset[object] = frozenset()

            def distance(
                left: Sequence[object],
                right: Sequence[object],
                kind: ColumnKind = column.kind,
            ) -> float | None:
                return _ks_distance(left, right, kind)

            metric_name = "ks_distance"
            omitted = {name: 0.0 for name in manifests}
        else:
            if grouping_scope == "dp_release":
                selected_categories = tuple(column.public_categories or ())
            else:
                selected_categories = _top_categories(
                    nonnull["real_train_eval"], config.categorical_top_k
                )
            categories = frozenset(selected_categories)

            def distance(
                left: Sequence[object],
                right: Sequence[object],
                selected: frozenset[object] = categories,
            ) -> float | None:
                return _tvd_distance(left, right, selected)

            metric_name = "tvd_distance"
            omitted = {name: _omitted_mass(values, categories) for name, values in nonnull.items()}

        try:
            baseline = distance(nonnull["real_train_eval"], nonnull["real_holdout"])
            synthetic_distance = distance(nonnull["real_holdout"], nonnull["synthetic"])
        except (ArithmeticError, TypeError, ValueError) as error:
            raise DomainError(
                ErrorCode.OUTPUT_INVALID,
                f"primary metric failed for column {column.name!r}: {error}",
            ) from error
        excess = (
            max(0.0, synthetic_distance - baseline)
            if baseline is not None and synthetic_distance is not None
            else None
        )
        missing_baseline = abs(
            _missing_rate(raw["real_train_eval"]) - _missing_rate(raw["real_holdout"])
        )
        missing_synthetic = abs(
            _missing_rate(raw["real_holdout"]) - _missing_rate(raw["synthetic"])
        )

        bootstrap_seed = config.derive_seed(f"primary-bootstrap:{column.name}")
        rng = random.Random(bootstrap_seed)
        baseline_boot: list[float] = []
        synthetic_boot: list[float] = []
        excess_boot: list[float] = []
        missing_baseline_boot: list[float] = []
        missing_synthetic_boot: list[float] = []
        for _ in range(config.bootstrap_repetitions):
            value_resamples = {
                name: _bootstrap_samples(values, rng) for name, values in nonnull.items()
            }
            raw_resamples = {name: _bootstrap_samples(values, rng) for name, values in raw.items()}
            baseline_rep = distance(
                value_resamples["real_train_eval"], value_resamples["real_holdout"]
            )
            synthetic_rep = distance(value_resamples["real_holdout"], value_resamples["synthetic"])
            if baseline_rep is not None:
                baseline_boot.append(baseline_rep)
            if synthetic_rep is not None:
                synthetic_boot.append(synthetic_rep)
            if baseline_rep is not None and synthetic_rep is not None:
                excess_boot.append(max(0.0, synthetic_rep - baseline_rep))
            missing_baseline_boot.append(
                abs(
                    _missing_rate(raw_resamples["real_train_eval"])
                    - _missing_rate(raw_resamples["real_holdout"])
                )
            )
            missing_synthetic_boot.append(
                abs(
                    _missing_rate(raw_resamples["real_holdout"])
                    - _missing_rate(raw_resamples["synthetic"])
                )
            )

        reason = "all_null_sample" if baseline is None or synthetic_distance is None else None
        result = ColumnPrimaryResult(
            name=column.name,
            kind=column.kind,
            included_in_fidelity_aggregate=excess is not None,
            baseline_distance=_estimate(
                metric=metric_name,
                value=baseline,
                bootstrap_values=baseline_boot,
                manifests=manifests,
                omitted_tail_mass=omitted,
                bootstrap_seed=bootstrap_seed,
                reason=reason,
            ),
            synthetic_distance=_estimate(
                metric=metric_name,
                value=synthetic_distance,
                bootstrap_values=synthetic_boot,
                manifests=manifests,
                omitted_tail_mass=omitted,
                bootstrap_seed=bootstrap_seed,
                reason=reason,
            ),
            baseline_excess=_estimate(
                metric="baseline_excess",
                value=excess,
                bootstrap_values=excess_boot,
                manifests=manifests,
                omitted_tail_mass=omitted,
                bootstrap_seed=bootstrap_seed,
                reason=reason,
            ),
            baseline_missingness_difference=_estimate(
                metric="missingness_absolute_difference",
                value=missing_baseline,
                bootstrap_values=missing_baseline_boot,
                manifests=manifests,
                omitted_tail_mass={name: 0.0 for name in manifests},
                bootstrap_seed=bootstrap_seed,
            ),
            synthetic_missingness_difference=_estimate(
                metric="missingness_absolute_difference",
                value=missing_synthetic,
                bootstrap_values=missing_synthetic_boot,
                manifests=manifests,
                omitted_tail_mass={name: 0.0 for name in manifests},
                bootstrap_seed=bootstrap_seed,
            ),
        )
        results.append(result)
        if excess is not None:
            eligible_names.append(column.name)
            eligible_excess.append(excess)
            eligible_bootstraps.append(excess_boot)

    aggregate_seed = config.derive_seed("primary-bootstrap:baseline-excess-aggregate")
    aggregate_boot: dict[str, list[float]] = {"median": [], "p95": [], "maximum": []}
    for repetition in range(config.bootstrap_repetitions):
        values = [
            bootstrap[repetition]
            for bootstrap in eligible_bootstraps
            if len(bootstrap) > repetition
        ]
        if values:
            aggregate_boot["median"].append(_quantile(values, 0.5))
            aggregate_boot["p95"].append(_quantile(values, 0.95))
            aggregate_boot["maximum"].append(max(values))
    aggregate_omitted = {name: 0.0 for name in manifests}

    def aggregate_metric(name: str, probability: float | None) -> MetricEstimate:
        if not eligible_excess:
            value = None
        elif probability is None:
            value = max(eligible_excess)
        else:
            value = _quantile(eligible_excess, probability)
        return _estimate(
            metric=f"baseline_excess_{name}",
            value=value,
            bootstrap_values=aggregate_boot[name],
            manifests=manifests,
            omitted_tail_mass=aggregate_omitted,
            bootstrap_seed=aggregate_seed,
            reason="no_eligible_fidelity_columns" if value is None else None,
        )

    aggregate = BaselineExcessAggregate(
        eligible_columns=tuple(eligible_names),
        median=aggregate_metric("median", 0.5),
        p95=aggregate_metric("p95", 0.95),
        maximum=aggregate_metric("maximum", None),
    )
    return PrimaryEvaluationResult(
        config_sha256=config.canonical_sha256,
        grouping_scope=grouping_scope,
        samples=manifests,
        columns=tuple(results),
        baseline_excess=aggregate,
    )
