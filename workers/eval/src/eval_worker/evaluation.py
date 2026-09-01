from __future__ import annotations

import hashlib
import heapq
import hmac
import importlib.util
import json
import math
import warnings
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from itertools import combinations
from typing import Any

import numpy as np
from scipy import sparse
from scipy.stats import spearmanr
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.feature_extraction import FeatureHasher
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, mean_squared_error, roc_auc_score
from sklearn.model_selection import train_test_split

EVALUATION_CONFIG_VERSION = "1.0"
PAIRWISE_SAMPLE_ROWS = 100_000
PAIRWISE_MAX_PAIRS = 2_415
C2ST_SAMPLE_ROWS = 50_000
C2ST_PREPROCESSING_ROWS = 200_000
DOWNSTREAM_SAMPLE_ROWS = 250_000
C2ST_SEEDS = 5
BOOTSTRAP_ITERATIONS = 500
GOWER_SAMPLE_ROWS = 5_000

_NUMERIC_KINDS = {"integer", "fixed_decimal", "float", "date", "datetime", "numeric"}
_CATEGORICAL_KINDS = {"categorical", "boolean", "code"}
_EXCLUDED_KINDS = {"identifier", "text", "excluded"}


@dataclass(frozen=True)
class Table:
    columns: dict[str, np.ndarray]
    row_count: int

    @classmethod
    def from_data(
        cls, data: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]]
    ) -> Table:
        if isinstance(data, cls):
            return data
        if isinstance(data, Mapping):
            columns = {str(name): np.asarray(values) for name, values in data.items()}
        else:
            records = list(data)
            if not records:
                return cls({}, 0)
            names = list(records[0])
            if any(list(record) != names for record in records):
                raise ValueError("all records must have identical column order")
            columns = {name: np.asarray([record[name] for record in records]) for name in names}
        lengths = {len(values) for values in columns.values()}
        if len(lengths) > 1:
            raise ValueError("table columns must have identical row counts")
        row_count = next(iter(lengths), 0)
        return cls(columns, row_count)

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(self.columns)

    def take(self, indices: np.ndarray) -> Table:
        return Table({name: values[indices] for name, values in self.columns.items()}, len(indices))

    def select(self, names: Sequence[str]) -> Table:
        return Table({name: self.columns[name] for name in names}, self.row_count)


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, np.datetime64):
        return bool(np.isnat(value))
    if isinstance(value, (float, np.floating)):
        return bool(np.isnan(value))
    return False


def _json_scalar(value: Any) -> Any:
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    return value


def _category_token(value: Any) -> str:
    if _is_missing(value):
        return '["missing"]'
    scalar = _json_scalar(value)
    return json.dumps(
        [type(scalar).__name__, scalar],
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _numeric_value(value: Any, kind: str) -> float:
    if _is_missing(value):
        return math.nan
    if kind in {"date", "datetime"}:
        try:
            unit = "D" if kind == "date" else "us"
            return float(np.datetime64(value, unit).astype("int64"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"invalid {kind} value: {value!r}") from exc
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"invalid numeric value: {value!r}") from exc
    if not math.isfinite(result):
        raise ValueError("numeric evaluation values must be finite or missing")
    return result


def _numeric_array(values: np.ndarray, kind: str) -> np.ndarray:
    return np.fromiter(
        (_numeric_value(value, kind) for value in values), dtype=np.float64, count=len(values)
    )


def _row_bytes(table: Table, index: int) -> bytes:
    cells: list[bytes] = []
    for name, values in table.columns.items():
        token = _category_token(values[index]).encode("utf-8")
        name_bytes = name.encode("utf-8")
        cells.append(len(name_bytes).to_bytes(4, "little") + name_bytes)
        cells.append(len(token).to_bytes(8, "little") + token)
    return b"".join(cells)


def derive_seed(master_seed: int | str, label: str) -> int:
    digest = hmac.new(
        str(master_seed).encode("utf-8"),
        label.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    return int.from_bytes(digest[:4], "big")


def deterministic_sample(
    table: Table, rows: int, seed: int | str, label: str
) -> tuple[Table, dict[str, Any]]:
    if rows < 0:
        raise ValueError("sample rows must be nonnegative")
    take = min(rows, table.row_count)
    key = hashlib.sha256(f"{seed}:{label}".encode()).digest()

    def priorities() -> Iterable[tuple[bytes, int]]:
        for index in range(table.row_count):
            yield hmac.new(key, _row_bytes(table, index), hashlib.sha256).digest(), index

    selected = heapq.nsmallest(take, priorities())
    indices = np.fromiter((index for _, index in selected), dtype=np.int64, count=take)
    digest = hashlib.sha256()
    for priority, index in selected:
        digest.update(priority)
        digest.update(index.to_bytes(8, "little"))
    return table.take(indices), {
        "method": "hmac_priority_uniform",
        "seed": int(seed) if isinstance(seed, int) else str(seed),
        "rows": take,
        "source_rows": table.row_count,
        "sample_hash": digest.hexdigest(),
        "omitted_tail_mass": None,
    }


def _not_estimated_ci() -> dict[str, Any]:
    return {
        "level": 0.95,
        "method": "not_estimated",
        "iterations": 0,
        "low": None,
        "high": None,
    }


def _percentile_ci(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return _not_estimated_ci()
    low, high = np.percentile(np.asarray(values, dtype=np.float64), [2.5, 97.5])
    return {
        "level": 0.95,
        "method": "percentile_bootstrap",
        "iterations": BOOTSTRAP_ITERATIONS,
        "low": float(low),
        "high": float(high),
    }


def _top_tokens(values: np.ndarray, limit: int) -> tuple[list[str], float]:
    tokens = [_category_token(value) for value in values]
    counts = Counter(tokens)
    ordered = sorted(counts, key=lambda token: (-counts[token], token))[:limit]
    retained = sum(counts[token] for token in ordered)
    omitted = 0.0 if not tokens else 1.0 - retained / len(tokens)
    return ordered, float(omitted)


def _group_tokens(values: np.ndarray, retained: set[str]) -> np.ndarray:
    return np.asarray(
        [token if token in retained else '["other"]' for token in map(_category_token, values)],
        dtype=str,
    )


def _tvd_from_counts(
    left: Counter[Any], right: Counter[Any], left_rows: int, right_rows: int
) -> float:
    if left_rows == 0 or right_rows == 0:
        return math.nan
    keys = left.keys() | right.keys()
    return 0.5 * sum(abs(left[key] / left_rows - right[key] / right_rows) for key in keys)


def _safe_correlation(values: np.ndarray, other: np.ndarray, *, spearman: bool) -> float | None:
    valid = ~(np.isnan(values) | np.isnan(other))
    if int(valid.sum()) < 2:
        return None
    left = values[valid]
    right = other[valid]
    if np.ptp(left) == 0 or np.ptp(right) == 0:
        return None
    if spearman:
        result = float(spearmanr(left, right).statistic)
    else:
        result = float(np.corrcoef(left, right)[0, 1])
    return result if math.isfinite(result) else None


def _correlation_difference(
    reference: Table, candidate: Table, left: str, right: str, kinds: Mapping[str, str]
) -> dict[str, Any]:
    reference_left = _numeric_array(reference.columns[left], kinds[left])
    reference_right = _numeric_array(reference.columns[right], kinds[right])
    candidate_left = _numeric_array(candidate.columns[left], kinds[left])
    candidate_right = _numeric_array(candidate.columns[right], kinds[right])
    ref_pearson = _safe_correlation(reference_left, reference_right, spearman=False)
    ref_spearman = _safe_correlation(reference_left, reference_right, spearman=True)
    cand_pearson = _safe_correlation(candidate_left, candidate_right, spearman=False)
    cand_spearman = _safe_correlation(candidate_left, candidate_right, spearman=True)
    return {
        "applicable": all(
            value is not None for value in (ref_pearson, ref_spearman, cand_pearson, cand_spearman)
        ),
        "pearson_reference": ref_pearson,
        "pearson_candidate": cand_pearson,
        "pearson_absolute_difference": None
        if ref_pearson is None or cand_pearson is None
        else abs(ref_pearson - cand_pearson),
        "spearman_reference": ref_spearman,
        "spearman_candidate": cand_spearman,
        "spearman_absolute_difference": None
        if ref_spearman is None or cand_spearman is None
        else abs(ref_spearman - cand_spearman),
        "omitted_tail_mass": 0.0,
        "ci": _not_estimated_ci(),
    }


def _categorical_pair_difference(
    reference: Table,
    candidate: Table,
    left: str,
    right: str,
    *,
    retained_left: Sequence[str],
    retained_right: Sequence[str],
    omitted_left: float,
    omitted_right: float,
) -> dict[str, Any]:
    reference_left = _group_tokens(reference.columns[left], set(retained_left))
    reference_right = _group_tokens(reference.columns[right], set(retained_right))
    candidate_left = _group_tokens(candidate.columns[left], set(retained_left))
    candidate_right = _group_tokens(candidate.columns[right], set(retained_right))
    ref_counts = Counter(zip(reference_left, reference_right, strict=True))
    cand_counts = Counter(zip(candidate_left, candidate_right, strict=True))
    return {
        "applicable": bool(reference.row_count and candidate.row_count),
        "tvd": _tvd_from_counts(ref_counts, cand_counts, reference.row_count, candidate.row_count),
        "grouping": {
            "source": "real_train_eval",
            "top_values_per_axis": 50,
            "left_retained": list(retained_left),
            "right_retained": list(retained_right),
            "contingency_cells_cap": 2_601,
        },
        "omitted_tail_mass": {"left": omitted_left, "right": omitted_right},
        "ci": _not_estimated_ci(),
    }


def _quantile_edges(values: np.ndarray, bins: int = 20) -> list[float]:
    finite = values[~np.isnan(values)]
    if finite.size == 0:
        return []
    raw = np.quantile(finite, np.linspace(0, 1, bins + 1)[1:-1])
    return [float(value) for value in np.unique(raw)]


def _numeric_bins(values: np.ndarray, edges: Sequence[float]) -> np.ndarray:
    output = np.searchsorted(np.asarray(edges, dtype=np.float64), values, side="right").astype(
        np.int64
    )
    output[np.isnan(values)] = len(edges) + 1
    return output


def _mixed_conditional_tvd(
    reference: Table,
    candidate: Table,
    numeric: str,
    categorical: str,
    kinds: Mapping[str, str],
    *,
    edges: Sequence[float],
    retained: Sequence[str],
    omitted: float,
    grouping_source: str,
) -> dict[str, Any]:
    ref_bins = _numeric_bins(_numeric_array(reference.columns[numeric], kinds[numeric]), edges)
    cand_bins = _numeric_bins(_numeric_array(candidate.columns[numeric], kinds[numeric]), edges)
    ref_cat = _group_tokens(reference.columns[categorical], set(retained))
    cand_cat = _group_tokens(candidate.columns[categorical], set(retained))
    result = 0.0
    for bin_id in sorted(set(ref_bins.tolist())):
        ref_mask = ref_bins == bin_id
        cand_mask = cand_bins == bin_id
        ref_rows = int(ref_mask.sum())
        if not ref_rows:
            continue
        cand_rows = int(cand_mask.sum())
        if not cand_rows:
            result += ref_rows / reference.row_count
            continue
        conditional = _tvd_from_counts(
            Counter(ref_cat[ref_mask]),
            Counter(cand_cat[cand_mask]),
            ref_rows,
            cand_rows,
        )
        result += ref_rows / reference.row_count * conditional
    return {
        "applicable": bool(reference.row_count and candidate.row_count and retained),
        "conditional_tvd": float(result),
        "grouping": {
            "source": grouping_source,
            "numeric_quantile_bins": 20,
            "numeric_edges": list(edges),
            "categorical_retained": list(retained),
        },
        "omitted_tail_mass": {"categorical": omitted},
        "ci": _not_estimated_ci(),
    }


def pairwise_metrics(
    real_train_eval: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    synthetic: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    column_types: Mapping[str, str],
    *,
    real_holdout: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]] | None = None,
    seed: int = 0,
    sample_rows: int = PAIRWISE_SAMPLE_ROWS,
    max_pairs: int = PAIRWISE_MAX_PAIRS,
    dp_release: bool = False,
    public_categories: Mapping[str, Sequence[Any]] | None = None,
    public_bins: Mapping[str, Sequence[float]] | None = None,
) -> dict[str, Any]:
    if max_pairs > PAIRWISE_MAX_PAIRS or max_pairs < 0:
        raise ValueError(f"max_pairs must be between 0 and {PAIRWISE_MAX_PAIRS}")
    real = Table.from_data(real_train_eval)
    synth = Table.from_data(synthetic)
    holdout = None if real_holdout is None else Table.from_data(real_holdout)
    columns = [
        name
        for name in real.column_names
        if name in synth.columns and column_types.get(name) not in _EXCLUDED_KINDS
    ]
    unknown = [
        name
        for name in columns
        if column_types.get(name) not in _NUMERIC_KINDS | _CATEGORICAL_KINDS
    ]
    if unknown:
        raise ValueError(f"unsupported pairwise column kinds: {unknown}")
    real_sample, real_meta = deterministic_sample(
        real.select(columns), sample_rows, seed, "pairwise-real-train"
    )
    synth_sample, synth_meta = deterministic_sample(
        synth.select(columns), sample_rows, seed, "pairwise-synthetic"
    )
    holdout_sample: Table | None = None
    holdout_meta: dict[str, Any] | None = None
    if holdout is not None:
        holdout_sample, holdout_meta = deterministic_sample(
            holdout.select(columns), sample_rows, seed, "pairwise-holdout"
        )

    public_categories = public_categories or {}
    public_bins = public_bins or {}
    all_pairs = list(combinations(columns, 2))
    selected_pairs = all_pairs[:max_pairs]
    results: list[dict[str, Any]] = []
    for left, right in selected_pairs:
        left_kind = column_types[left]
        right_kind = column_types[right]
        if left_kind in _NUMERIC_KINDS and right_kind in _NUMERIC_KINDS:
            synthetic_metric = _correlation_difference(
                real_sample, synth_sample, left, right, column_types
            )
            control_metric = (
                None
                if holdout_sample is None
                else _correlation_difference(real_sample, holdout_sample, left, right, column_types)
            )
            family = "numeric_numeric"
        elif left_kind in _CATEGORICAL_KINDS and right_kind in _CATEGORICAL_KINDS:
            if dp_release and (left not in public_categories or right not in public_categories):
                results.append(
                    {
                        "columns": [left, right],
                        "family": "categorical_categorical",
                        "applicable": False,
                        "reason": "private_grouping_forbidden_for_dp_release",
                        "seed": seed,
                        "sample_hashes": {
                            "real_train_eval": real_meta["sample_hash"],
                            "synthetic": synth_meta["sample_hash"],
                        },
                        "rows": {
                            "real_train_eval": real_sample.row_count,
                            "synthetic": synth_sample.row_count,
                        },
                        "omitted_tail_mass": None,
                        "ci": _not_estimated_ci(),
                    }
                )
                continue
            if dp_release:
                retained_left = [_category_token(value) for value in public_categories[left]][:50]
                retained_right = [_category_token(value) for value in public_categories[right]][:50]
                omitted_left = omitted_right = 0.0
            else:
                retained_left, omitted_left = _top_tokens(real_sample.columns[left], 50)
                retained_right, omitted_right = _top_tokens(real_sample.columns[right], 50)
            synthetic_metric = _categorical_pair_difference(
                real_sample,
                synth_sample,
                left,
                right,
                retained_left=retained_left,
                retained_right=retained_right,
                omitted_left=omitted_left,
                omitted_right=omitted_right,
            )
            control_metric = (
                None
                if holdout_sample is None
                else _categorical_pair_difference(
                    real_sample,
                    holdout_sample,
                    left,
                    right,
                    retained_left=retained_left,
                    retained_right=retained_right,
                    omitted_left=omitted_left,
                    omitted_right=omitted_right,
                )
            )
            family = "categorical_categorical"
        else:
            numeric, categorical = (left, right) if left_kind in _NUMERIC_KINDS else (right, left)
            if dp_release and (numeric not in public_bins or categorical not in public_categories):
                results.append(
                    {
                        "columns": [left, right],
                        "family": "mixed",
                        "applicable": False,
                        "reason": "private_grouping_forbidden_for_dp_release",
                        "seed": seed,
                        "sample_hashes": {
                            "real_train_eval": real_meta["sample_hash"],
                            "synthetic": synth_meta["sample_hash"],
                        },
                        "rows": {
                            "real_train_eval": real_sample.row_count,
                            "synthetic": synth_sample.row_count,
                        },
                        "omitted_tail_mass": None,
                        "ci": _not_estimated_ci(),
                    }
                )
                continue
            if dp_release:
                edges = [float(value) for value in public_bins[numeric]]
                retained = [_category_token(value) for value in public_categories[categorical]][:50]
                omitted = 0.0
                grouping_source = "public_metadata"
            else:
                edges = _quantile_edges(
                    _numeric_array(real_sample.columns[numeric], column_types[numeric])
                )
                retained, omitted = _top_tokens(real_sample.columns[categorical], 50)
                grouping_source = "real_train_eval"
            synthetic_metric = _mixed_conditional_tvd(
                real_sample,
                synth_sample,
                numeric,
                categorical,
                column_types,
                edges=edges,
                retained=retained,
                omitted=omitted,
                grouping_source=grouping_source,
            )
            control_metric = (
                None
                if holdout_sample is None
                else _mixed_conditional_tvd(
                    real_sample,
                    holdout_sample,
                    numeric,
                    categorical,
                    column_types,
                    edges=edges,
                    retained=retained,
                    omitted=omitted,
                    grouping_source=grouping_source,
                )
            )
            family = "mixed"
        results.append(
            {
                "columns": [left, right],
                "family": family,
                "applicable": synthetic_metric["applicable"],
                "synthetic_difference": synthetic_metric,
                "holdout_control_difference": control_metric,
                "seed": seed,
                "sample_hashes": {
                    "real_train_eval": real_meta["sample_hash"],
                    "synthetic": synth_meta["sample_hash"],
                    **({"real_holdout": holdout_meta["sample_hash"]} if holdout_meta else {}),
                },
                "rows": {
                    "real_train_eval": real_sample.row_count,
                    "synthetic": synth_sample.row_count,
                    **(
                        {"real_holdout": holdout_sample.row_count}
                        if holdout_sample is not None
                        else {}
                    ),
                },
                "omitted_tail_mass": synthetic_metric["omitted_tail_mass"],
                "ci": synthetic_metric["ci"],
            }
        )
    return {
        "applicable": bool(selected_pairs),
        "seed": seed,
        "sample_hashes": {
            "real_train_eval": real_meta["sample_hash"],
            "synthetic": synth_meta["sample_hash"],
            **({"real_holdout": holdout_meta["sample_hash"]} if holdout_meta else {}),
        },
        "rows": {
            "real_train_eval": real_sample.row_count,
            "synthetic": synth_sample.row_count,
            **({"real_holdout": holdout_sample.row_count} if holdout_sample is not None else {}),
        },
        "pair_cap": PAIRWISE_MAX_PAIRS,
        "pairs_considered": len(selected_pairs),
        "pairs_omitted": len(all_pairs) - len(selected_pairs),
        "omitted_tail_mass": None,
        "ci": _not_estimated_ci(),
        "pairs": results,
    }


@dataclass
class LinearPreprocessor:
    numeric_columns: list[str]
    categorical_columns: list[str]
    column_types: Mapping[str, str]
    medians: np.ndarray | None = None
    means: np.ndarray | None = None
    scales: np.ndarray | None = None
    hasher: FeatureHasher | None = None

    def fit(self, table: Table) -> LinearPreprocessor:
        numeric = self._numeric_matrix(table)
        if numeric.shape[1]:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                medians = np.nanmedian(numeric, axis=0)
            medians = np.where(np.isnan(medians), 0.0, medians)
            imputed = np.where(np.isnan(numeric), medians, numeric)
            means = imputed.mean(axis=0)
            scales = imputed.std(axis=0)
            scales[scales == 0] = 1.0
            self.medians, self.means, self.scales = medians, means, scales
        else:
            self.medians = self.means = self.scales = np.empty(0, dtype=np.float64)
        self.hasher = FeatureHasher(n_features=4_096, input_type="string")
        return self

    def _numeric_matrix(self, table: Table) -> np.ndarray:
        if not self.numeric_columns:
            return np.empty((table.row_count, 0), dtype=np.float64)
        return np.column_stack(
            [
                _numeric_array(table.columns[name], self.column_types[name])
                for name in self.numeric_columns
            ]
        )

    def transform(self, table: Table) -> sparse.csr_matrix:
        if self.medians is None or self.means is None or self.scales is None or self.hasher is None:
            raise RuntimeError("preprocessor is not fitted")
        numeric = self._numeric_matrix(table)
        imputed = np.where(np.isnan(numeric), self.medians, numeric)
        normalized = (imputed - self.means) / self.scales
        numeric_sparse = sparse.csr_matrix(normalized)
        categorical_rows = (
            [
                f"{name}={_category_token(table.columns[name][row])}"
                for name in self.categorical_columns
            ]
            for row in range(table.row_count)
        )
        hashed = self.hasher.transform(categorical_rows)
        return sparse.hstack((numeric_sparse, hashed), format="csr")

    def metadata(self) -> dict[str, Any]:
        return {
            "fit_dataset": "real_train_eval",
            "numeric_columns": self.numeric_columns,
            "categorical_columns": self.categorical_columns,
            "numeric_medians": [] if self.medians is None else self.medians.tolist(),
            "numeric_means": [] if self.means is None else self.means.tolist(),
            "numeric_scales": [] if self.scales is None else self.scales.tolist(),
            "categorical_hash_dimensions": 4_096,
        }


@dataclass
class NonlinearPreprocessor:
    numeric_columns: list[str]
    categorical_columns: list[str]
    column_types: Mapping[str, str]
    medians: np.ndarray | None = None
    categories: dict[str, dict[str, int]] | None = None

    def fit(self, table: Table) -> NonlinearPreprocessor:
        if self.numeric_columns:
            numeric = np.column_stack(
                [
                    _numeric_array(table.columns[name], self.column_types[name])
                    for name in self.numeric_columns
                ]
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                medians = np.nanmedian(numeric, axis=0)
            self.medians = np.where(np.isnan(medians), 0.0, medians)
        else:
            self.medians = np.empty(0, dtype=np.float64)
        self.categories = {}
        for name in self.categorical_columns:
            top, _ = _top_tokens(table.columns[name], 255)
            self.categories[name] = {token: index for index, token in enumerate(top)}
        return self

    @property
    def categorical_mask(self) -> list[bool]:
        return [False] * len(self.numeric_columns) + [True] * len(self.categorical_columns)

    def transform(self, table: Table) -> np.ndarray:
        if self.medians is None or self.categories is None:
            raise RuntimeError("preprocessor is not fitted")
        if self.numeric_columns:
            numeric = np.column_stack(
                [
                    _numeric_array(table.columns[name], self.column_types[name])
                    for name in self.numeric_columns
                ]
            )
            numeric = np.where(np.isnan(numeric), self.medians, numeric)
        else:
            numeric = np.empty((table.row_count, 0), dtype=np.float64)
        categorical_parts: list[np.ndarray] = []
        for name in self.categorical_columns:
            mapping = self.categories[name]
            categorical_parts.append(
                np.fromiter(
                    (
                        float(mapping[token])
                        if (token := _category_token(value)) in mapping
                        else math.nan
                        for value in table.columns[name]
                    ),
                    dtype=np.float64,
                    count=table.row_count,
                )
            )
        categorical = (
            np.column_stack(categorical_parts)
            if categorical_parts
            else np.empty((table.row_count, 0))
        )
        return np.hstack((numeric, categorical))

    def metadata(self) -> dict[str, Any]:
        return {
            "fit_dataset": "real_train_eval",
            "numeric_columns": self.numeric_columns,
            "categorical_columns": self.categorical_columns,
            "numeric_medians": [] if self.medians is None else self.medians.tolist(),
            "categorical_top_limit": 255,
            "categorical_unknown_sentinel": "NaN",
            "category_counts": {}
            if self.categories is None
            else {name: len(values) for name, values in self.categories.items()},
        }


def _feature_columns(
    table: Table, column_types: Mapping[str, str], target: str | None = None
) -> tuple[list[str], list[str]]:
    numeric: list[str] = []
    categorical: list[str] = []
    for name in table.column_names:
        if name == target:
            continue
        kind = column_types.get(name)
        if kind in _NUMERIC_KINDS:
            numeric.append(name)
        elif kind in _CATEGORICAL_KINDS:
            categorical.append(name)
    if not numeric and not categorical:
        raise ValueError("evaluation requires at least one eligible feature column")
    return numeric, categorical


def _stack(
    left: sparse.spmatrix | np.ndarray, right: sparse.spmatrix | np.ndarray
) -> sparse.spmatrix | np.ndarray:
    if sparse.issparse(left) or sparse.issparse(right):
        return sparse.vstack((left, right), format="csr")
    return np.vstack((left, right))


def _auc_bootstrap(
    runs: Sequence[tuple[np.ndarray, np.ndarray]],
    seed: int,
) -> dict[str, Any]:
    rng = np.random.default_rng(seed)
    estimates: list[float] = []
    for _ in range(BOOTSTRAP_ITERATIONS):
        run_aucs: list[float] = []
        for labels, scores in runs:
            negative = np.flatnonzero(labels == 0)
            positive = np.flatnonzero(labels == 1)
            selected = np.concatenate(
                (
                    rng.choice(negative, len(negative), replace=True),
                    rng.choice(positive, len(positive), replace=True),
                )
            )
            run_aucs.append(float(roc_auc_score(labels[selected], scores[selected])))
        estimates.append(float(np.mean(run_aucs)))
    return _percentile_ci(estimates)


def _c2st_model_summary(
    left_features: sparse.spmatrix | np.ndarray,
    right_features: sparse.spmatrix | np.ndarray,
    *,
    family: str,
    categorical_mask: Sequence[bool] | None,
    model_seeds: Sequence[int],
    bootstrap_seed: int,
) -> dict[str, Any]:
    features = _stack(left_features, right_features)
    labels = np.concatenate(
        (
            np.zeros(left_features.shape[0], dtype=np.int8),
            np.ones(right_features.shape[0], dtype=np.int8),
        )
    )
    aucs: list[float] = []
    prediction_runs: list[tuple[np.ndarray, np.ndarray]] = []
    for model_seed in model_seeds:
        train_indices, test_indices = train_test_split(
            np.arange(len(labels)),
            test_size=0.30,
            stratify=labels,
            random_state=model_seed,
        )
        if family == "linear":
            model = LogisticRegression(
                solver="saga",
                C=1,
                max_iter=500,
                class_weight="balanced",
                n_jobs=1,
                random_state=model_seed,
            )
        else:
            model = HistGradientBoostingClassifier(
                max_iter=200,
                learning_rate=0.05,
                max_leaf_nodes=31,
                min_samples_leaf=50,
                l2_regularization=1,
                categorical_features=list(categorical_mask or []),
                random_state=model_seed,
            )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(features[train_indices], labels[train_indices])
        scores = model.predict_proba(features[test_indices])[:, 1]
        test_labels = labels[test_indices]
        aucs.append(float(roc_auc_score(test_labels, scores)))
        prediction_runs.append((test_labels, scores))
    settings = (
        {"solver": "saga", "C": 1, "max_iter": 500, "class_weight": "balanced", "n_jobs": 1}
        if family == "linear"
        else {
            "max_iter": 200,
            "learning_rate": 0.05,
            "max_leaf_nodes": 31,
            "min_samples_leaf": 50,
            "l2_regularization": 1,
            "categorical_features": list(categorical_mask or []),
        }
    )
    return {
        "applicable": True,
        "model_family": family,
        "settings": settings,
        "split": {"train_fraction": 0.70, "test_fraction": 0.30, "stratified": True},
        "seeds": list(model_seeds),
        "auroc_by_seed": aucs,
        "auroc": float(np.mean(aucs)),
        "ci": _auc_bootstrap(prediction_runs, bootstrap_seed),
        "bootstrap_seed": bootstrap_seed,
        "omitted_tail_mass": None,
    }


def c2st_metrics(
    real_train_eval: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    real_holdout: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    synthetic: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    column_types: Mapping[str, str],
    *,
    seed: int = 0,
    sample_rows: int = C2ST_SAMPLE_ROWS,
) -> dict[str, Any]:
    real = Table.from_data(real_train_eval)
    holdout = Table.from_data(real_holdout)
    synth = Table.from_data(synthetic)
    numeric, categorical = _feature_columns(real, column_types)
    features = numeric + categorical
    prep_sample, prep_meta = deterministic_sample(
        real.select(features), C2ST_PREPROCESSING_ROWS, seed, "c2st-preprocessing-real-train"
    )
    comparison_rows = min(sample_rows, real.row_count, holdout.row_count, synth.row_count)
    holdout_sample, holdout_meta = deterministic_sample(
        holdout.select(features), comparison_rows, seed, "c2st-holdout"
    )
    synth_sample, synth_meta = deterministic_sample(
        synth.select(features), comparison_rows, seed, "c2st-synthetic"
    )
    control_real, control_meta = deterministic_sample(
        real.select(features), comparison_rows, seed, "c2st-control-real-train"
    )
    if comparison_rows < 4:
        return {
            "applicable": False,
            "reason": "each C2ST population requires at least four rows",
            "seed": seed,
            "sample_hashes": {
                "real_train_eval": prep_meta["sample_hash"],
                "real_holdout": holdout_meta["sample_hash"],
                "synthetic": synth_meta["sample_hash"],
            },
            "rows": {
                "real_train_eval": prep_sample.row_count,
                "real_holdout": holdout_sample.row_count,
                "synthetic": synth_sample.row_count,
            },
            "omitted_tail_mass": None,
            "ci": _not_estimated_ci(),
        }

    linear = LinearPreprocessor(numeric, categorical, column_types).fit(prep_sample)
    nonlinear = NonlinearPreprocessor(numeric, categorical, column_types).fit(prep_sample)
    model_seeds = [derive_seed(seed, f"c2st-model-{index}") for index in range(C2ST_SEEDS)]
    synthetic_comparison = {
        "linear": _c2st_model_summary(
            linear.transform(holdout_sample),
            linear.transform(synth_sample),
            family="linear",
            categorical_mask=None,
            model_seeds=model_seeds,
            bootstrap_seed=derive_seed(seed, "c2st-synthetic-linear-bootstrap"),
        ),
        "nonlinear": _c2st_model_summary(
            nonlinear.transform(holdout_sample),
            nonlinear.transform(synth_sample),
            family="nonlinear",
            categorical_mask=nonlinear.categorical_mask,
            model_seeds=model_seeds,
            bootstrap_seed=derive_seed(seed, "c2st-synthetic-nonlinear-bootstrap"),
        ),
    }
    control_comparison = {
        "linear": _c2st_model_summary(
            linear.transform(control_real),
            linear.transform(holdout_sample),
            family="linear",
            categorical_mask=None,
            model_seeds=model_seeds,
            bootstrap_seed=derive_seed(seed, "c2st-control-linear-bootstrap"),
        ),
        "nonlinear": _c2st_model_summary(
            nonlinear.transform(control_real),
            nonlinear.transform(holdout_sample),
            family="nonlinear",
            categorical_mask=nonlinear.categorical_mask,
            model_seeds=model_seeds,
            bootstrap_seed=derive_seed(seed, "c2st-control-nonlinear-bootstrap"),
        ),
    }
    return {
        "applicable": True,
        "seed": seed,
        "seeds": model_seeds,
        "sample_hashes": {
            "preprocessing_real_train_eval": prep_meta["sample_hash"],
            "control_real_train_eval": control_meta["sample_hash"],
            "real_holdout": holdout_meta["sample_hash"],
            "synthetic": synth_meta["sample_hash"],
        },
        "rows": {
            "preprocessing_real_train_eval": prep_sample.row_count,
            "comparison_per_population": comparison_rows,
        },
        "preprocessing": {
            "fit_dataset": "real_train_eval",
            "holdout_used_for_fit": False,
            "synthetic_used_for_fit": False,
            "linear": linear.metadata(),
            "nonlinear": nonlinear.metadata(),
        },
        "synthetic_vs_untouched_holdout": synthetic_comparison,
        "real_train_eval_vs_untouched_holdout_control": control_comparison,
        "omitted_tail_mass": None,
        "ci": {
            "synthetic_linear": synthetic_comparison["linear"]["ci"],
            "synthetic_nonlinear": synthetic_comparison["nonlinear"]["ci"],
            "control_linear": control_comparison["linear"]["ci"],
            "control_nonlinear": control_comparison["nonlinear"]["ci"],
        },
    }


def downstream_utility_metrics(
    real_train_eval: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    real_holdout: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    synthetic: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    column_types: Mapping[str, str],
    *,
    target: str | None,
    task: str | None,
    seed: int = 0,
) -> dict[str, Any]:
    if target is None and task is None:
        return {
            "applicable": False,
            "reason": "explicit_target_and_task_required",
            "automatic_target_selection": False,
            "seed": seed,
            "sample_hashes": {},
            "rows": {},
            "omitted_tail_mass": None,
            "ci": _not_estimated_ci(),
        }
    if target is None or task not in {"classification", "regression"}:
        raise ValueError("target and task=classification|regression must be supplied together")

    real_source = Table.from_data(real_train_eval)
    holdout_source = Table.from_data(real_holdout)
    synth_source = Table.from_data(synthetic)
    if any(target not in table.columns for table in (real_source, holdout_source, synth_source)):
        raise ValueError("utility target must exist in all datasets")
    if any(
        table.column_names != real_source.column_names for table in (holdout_source, synth_source)
    ):
        raise ValueError("utility datasets must have identical column order")
    real, real_meta = deterministic_sample(
        real_source, DOWNSTREAM_SAMPLE_ROWS, seed, "downstream-real-train"
    )
    holdout, holdout_meta = deterministic_sample(
        holdout_source, DOWNSTREAM_SAMPLE_ROWS, seed, "downstream-holdout"
    )
    synth, synth_meta = deterministic_sample(
        synth_source, DOWNSTREAM_SAMPLE_ROWS, seed, "downstream-synthetic"
    )
    sample_hashes = {
        "real_train_eval": real_meta["sample_hash"],
        "real_holdout": holdout_meta["sample_hash"],
        "synthetic": synth_meta["sample_hash"],
    }
    sampled_rows = {
        "real_train_eval": real.row_count,
        "real_holdout": holdout.row_count,
        "synthetic": synth.row_count,
    }

    def not_applicable(reason: str) -> dict[str, Any]:
        return {
            "applicable": False,
            "reason": reason,
            "target": target,
            "task": task,
            "automatic_target_selection": False,
            "seed": seed,
            "sample_hashes": sample_hashes,
            "rows": sampled_rows,
            "omitted_tail_mass": None,
            "ci": _not_estimated_ci(),
        }

    numeric, categorical = _feature_columns(real, column_types, target)
    feature_names = numeric + categorical
    model_seed = derive_seed(seed, "downstream-utility")
    if task == "classification":
        prep: LinearPreprocessor | NonlinearPreprocessor = LinearPreprocessor(
            numeric, categorical, column_types
        ).fit(real.select(feature_names))
        real_x = prep.transform(real.select(feature_names))
        synth_x = prep.transform(synth.select(feature_names))
        holdout_x = prep.transform(holdout.select(feature_names))
        labels = sorted({_category_token(value) for value in real.columns[target]})
        label_map = {label: index for index, label in enumerate(labels)}
        if len(labels) < 2:
            return not_applicable("classification_target_is_constant")
        real_y = np.asarray([label_map[_category_token(value)] for value in real.columns[target]])
        synth_tokens = np.asarray([_category_token(value) for value in synth.columns[target]])
        synth_valid = np.asarray([token in label_map for token in synth_tokens])
        synth_y = np.asarray([label_map[token] for token in synth_tokens[synth_valid]])
        holdout_tokens = np.asarray([_category_token(value) for value in holdout.columns[target]])
        holdout_valid = np.asarray([token in label_map for token in holdout_tokens])
        holdout_y = np.asarray([label_map[token] for token in holdout_tokens[holdout_valid]])
        if len(np.unique(synth_y)) < 2:
            return not_applicable("synthetic_target_has_fewer_than_two_known_classes")
        if not holdout_y.size:
            return not_applicable("holdout_has_no_known_target_classes")
        settings = {
            "solver": "saga",
            "C": 1,
            "max_iter": 500,
            "class_weight": "balanced",
            "n_jobs": 1,
        }
        trtr_model = LogisticRegression(**settings, random_state=model_seed)
        tstr_model = LogisticRegression(**settings, random_state=model_seed)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            trtr_model.fit(real_x, real_y)
            tstr_model.fit(synth_x[synth_valid], synth_y)
        trtr_score = float(accuracy_score(holdout_y, trtr_model.predict(holdout_x[holdout_valid])))
        tstr_score = float(accuracy_score(holdout_y, tstr_model.predict(holdout_x[holdout_valid])))
        metric = "accuracy"
    else:
        if column_types.get(target) not in _NUMERIC_KINDS:
            raise ValueError("regression target must have a numeric/date/datetime kind")
        prep = NonlinearPreprocessor(numeric, categorical, column_types).fit(
            real.select(feature_names)
        )
        real_x = prep.transform(real.select(feature_names))
        synth_x = prep.transform(synth.select(feature_names))
        holdout_x = prep.transform(holdout.select(feature_names))
        real_y = _numeric_array(real.columns[target], column_types[target])
        synth_y = _numeric_array(synth.columns[target], column_types[target])
        holdout_y = _numeric_array(holdout.columns[target], column_types[target])
        real_valid, synth_valid, holdout_valid = (
            ~np.isnan(real_y),
            ~np.isnan(synth_y),
            ~np.isnan(holdout_y),
        )
        if real_valid.sum() < 2 or synth_valid.sum() < 2 or not holdout_valid.any():
            return not_applicable("regression_target_has_insufficient_nonmissing_rows")
        settings = {
            "max_iter": 200,
            "learning_rate": 0.05,
            "max_leaf_nodes": 31,
            "min_samples_leaf": 50,
            "l2_regularization": 1,
        }
        trtr_model = HistGradientBoostingRegressor(**settings, random_state=model_seed)
        tstr_model = HistGradientBoostingRegressor(**settings, random_state=model_seed)
        trtr_model.fit(real_x[real_valid], real_y[real_valid])
        tstr_model.fit(synth_x[synth_valid], synth_y[synth_valid])
        trtr_score = float(
            math.sqrt(
                mean_squared_error(
                    holdout_y[holdout_valid], trtr_model.predict(holdout_x[holdout_valid])
                )
            )
        )
        tstr_score = float(
            math.sqrt(
                mean_squared_error(
                    holdout_y[holdout_valid], tstr_model.predict(holdout_x[holdout_valid])
                )
            )
        )
        metric = "rmse"
    return {
        "applicable": True,
        "target": target,
        "task": task,
        "automatic_target_selection": False,
        "preprocessing": {
            "fit_dataset": "real_train_eval",
            "holdout_used_for_fit": False,
            "synthetic_used_for_fit": False,
            "details": prep.metadata(),
        },
        "model_settings": settings,
        "metric": metric,
        "trtr": trtr_score,
        "tstr": tstr_score,
        "difference_tstr_minus_trtr": tstr_score - trtr_score,
        "seed": seed,
        "model_seed": model_seed,
        "sample_hashes": sample_hashes,
        "rows": sampled_rows,
        "omitted_tail_mass": None,
        "ci": _not_estimated_ci(),
    }


def _gower_column_distance(
    queries: np.ndarray,
    train: np.ndarray,
    kind: str,
    numeric_bounds: tuple[float, float] | None,
) -> np.ndarray:
    query_missing = np.asarray([_is_missing(value) for value in queries])
    train_missing = np.asarray([_is_missing(value) for value in train])
    mismatch_missing = query_missing[:, None] != train_missing[None, :]
    both_present = ~(query_missing[:, None] | train_missing[None, :])
    if kind in _NUMERIC_KINDS:
        assert numeric_bounds is not None
        low, high = numeric_bounds
        query_numeric = _numeric_array(queries, kind)
        train_numeric = _numeric_array(train, kind)
        if high > low:
            query_numeric = np.clip(query_numeric, low, high)
            train_numeric = np.clip(train_numeric, low, high)
            distance = np.abs(query_numeric[:, None] - train_numeric[None, :]) / (high - low)
        else:
            distance = np.zeros((len(queries), len(train)), dtype=np.float64)
    else:
        query_tokens = np.asarray([_category_token(value) for value in queries])
        train_tokens = np.asarray([_category_token(value) for value in train])
        distance = (query_tokens[:, None] != train_tokens[None, :]).astype(np.float64)
    distance[~both_present] = 0.0
    distance[mismatch_missing] = 1.0
    return distance


def exact_gower_nearest(
    train: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    queries: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    column_types: Mapping[str, str],
    *,
    block_rows: int = 128,
) -> tuple[np.ndarray, np.ndarray, dict[str, tuple[float, float]]]:
    train_table = Table.from_data(train)
    query_table = Table.from_data(queries)
    columns = [
        name
        for name in train_table.column_names
        if name in query_table.columns
        and column_types.get(name) in _NUMERIC_KINDS | _CATEGORICAL_KINDS
    ]
    if not columns:
        raise ValueError("Gower distance requires an eligible non-identifier/non-text column")
    if train_table.row_count < 2:
        raise ValueError("Gower NNDR requires at least two training rows")
    bounds: dict[str, tuple[float, float]] = {}
    for name in columns:
        if column_types[name] in _NUMERIC_KINDS:
            values = _numeric_array(train_table.columns[name], column_types[name])
            finite = values[~np.isnan(values)]
            bounds[name] = (
                (0.0, 0.0)
                if not finite.size
                else tuple(float(value) for value in np.percentile(finite, [1, 99]))
            )
    first = np.empty(query_table.row_count, dtype=np.float64)
    second = np.empty(query_table.row_count, dtype=np.float64)
    for start in range(0, query_table.row_count, block_rows):
        stop = min(query_table.row_count, start + block_rows)
        distances = np.zeros((stop - start, train_table.row_count), dtype=np.float64)
        for name in columns:
            distances += _gower_column_distance(
                query_table.columns[name][start:stop],
                train_table.columns[name],
                column_types[name],
                bounds.get(name),
            )
        distances /= len(columns)
        nearest = np.partition(distances, kth=1, axis=1)[:, :2]
        nearest.sort(axis=1)
        first[start:stop], second[start:stop] = nearest[:, 0], nearest[:, 1]
    return first, second, bounds


def _distribution_summary(values: np.ndarray) -> dict[str, float]:
    quantiles = np.quantile(values, [0, 0.05, 0.25, 0.5, 0.75, 0.95, 1])
    return dict(
        zip(
            ("min", "p05", "p25", "median", "p75", "p95", "max"), map(float, quantiles), strict=True
        )
    )


def _bootstrap_median(values: np.ndarray, seed: int) -> dict[str, Any]:
    rng = np.random.default_rng(seed)
    estimates = [
        float(np.median(rng.choice(values, len(values), replace=True)))
        for _ in range(BOOTSTRAP_ITERATIONS)
    ]
    return _percentile_ci(estimates)


def _bootstrap_median_difference(left: np.ndarray, right: np.ndarray, seed: int) -> dict[str, Any]:
    rng = np.random.default_rng(seed)
    estimates = [
        float(
            np.median(rng.choice(left, len(left), replace=True))
            - np.median(rng.choice(right, len(right), replace=True))
        )
        for _ in range(BOOTSTRAP_ITERATIONS)
    ]
    return _percentile_ci(estimates)


def gower_privacy_metrics(
    real_train: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    real_holdout: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    synthetic: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    column_types: Mapping[str, str],
    *,
    seed: int = 0,
    sample_rows: int = GOWER_SAMPLE_ROWS,
    block_rows: int = 128,
) -> dict[str, Any]:
    train = Table.from_data(real_train)
    holdout = Table.from_data(real_holdout)
    synth = Table.from_data(synthetic)
    columns = [
        name
        for name in train.column_names
        if column_types.get(name) in _NUMERIC_KINDS | _CATEGORICAL_KINDS
    ]
    train_sample, train_meta = deterministic_sample(
        train.select(columns), sample_rows, seed, "gower-real-train"
    )
    holdout_sample, holdout_meta = deterministic_sample(
        holdout.select(columns), sample_rows, seed, "gower-holdout"
    )
    synth_sample, synth_meta = deterministic_sample(
        synth.select(columns), sample_rows, seed, "gower-synthetic"
    )
    synth_dcr, synth_second, bounds = exact_gower_nearest(
        train_sample, synth_sample, column_types, block_rows=block_rows
    )
    holdout_dcr, holdout_second, _ = exact_gower_nearest(
        train_sample, holdout_sample, column_types, block_rows=block_rows
    )
    synth_nndr = np.divide(
        synth_dcr, synth_second, out=np.zeros_like(synth_dcr), where=synth_second > 0
    )
    holdout_nndr = np.divide(
        holdout_dcr, holdout_second, out=np.zeros_like(holdout_dcr), where=holdout_second > 0
    )
    dcr_seed = derive_seed(seed, "gower-dcr-bootstrap")
    nndr_seed = derive_seed(seed, "gower-nndr-bootstrap")
    return {
        "applicable": True,
        "diagnostic_type": "empirical_attack_diagnostic",
        "formal_privacy_guarantee": False,
        "seed": seed,
        "bootstrap_seeds": {"dcr": dcr_seed, "nndr": nndr_seed},
        "sample_hashes": {
            "real_train": train_meta["sample_hash"],
            "real_holdout": holdout_meta["sample_hash"],
            "synthetic": synth_meta["sample_hash"],
        },
        "rows": {
            "real_train": train_sample.row_count,
            "real_holdout": holdout_sample.row_count,
            "synthetic": synth_sample.row_count,
        },
        "columns": columns,
        "excluded_column_kinds": sorted(_EXCLUDED_KINDS),
        "numeric_clip_bounds_real_train_p01_p99": {
            name: list(value) for name, value in bounds.items()
        },
        "distance": "blockwise_exact_gower",
        "dcr": {
            "synthetic_to_train": _distribution_summary(synth_dcr),
            "holdout_to_train_control": _distribution_summary(holdout_dcr),
            "synthetic_median_minus_control": float(np.median(synth_dcr) - np.median(holdout_dcr)),
            "synthetic_median_ci": _bootstrap_median(synth_dcr, dcr_seed),
            "control_median_ci": _bootstrap_median(
                holdout_dcr, derive_seed(seed, "gower-control-dcr-bootstrap")
            ),
            "difference_ci": _bootstrap_median_difference(
                synth_dcr, holdout_dcr, derive_seed(seed, "gower-dcr-difference-bootstrap")
            ),
        },
        "nndr": {
            "synthetic_to_train": _distribution_summary(synth_nndr),
            "holdout_to_train_control": _distribution_summary(holdout_nndr),
            "synthetic_median_minus_control": float(
                np.median(synth_nndr) - np.median(holdout_nndr)
            ),
            "synthetic_median_ci": _bootstrap_median(synth_nndr, nndr_seed),
            "control_median_ci": _bootstrap_median(
                holdout_nndr, derive_seed(seed, "gower-control-nndr-bootstrap")
            ),
            "difference_ci": _bootstrap_median_difference(
                synth_nndr, holdout_nndr, derive_seed(seed, "gower-nndr-difference-bootstrap")
            ),
        },
        "omitted_tail_mass": None,
        "ci": {
            "dcr": _bootstrap_median(synth_dcr, dcr_seed),
            "nndr": _bootstrap_median(synth_nndr, nndr_seed),
        },
    }


def anonymeter_metrics(
    real_train: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]] | None = None,
    real_holdout: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]] | None = None,
    synthetic: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]] | None = None,
    *,
    secret_groups: Sequence[Sequence[str]] | None,
    auxiliary_groups: Sequence[Sequence[str]] | None,
    seed: int = 0,
) -> dict[str, Any]:
    explicit = bool(secret_groups) and bool(auxiliary_groups)
    dependency_available = importlib.util.find_spec("anonymeter") is not None
    seeds = [derive_seed(seed, f"anonymeter-{index}") for index in range(3)]
    unavailable = {
        "applicable": False,
        "dependency_available": dependency_available,
        "explicit_secret_groups": bool(secret_groups),
        "explicit_auxiliary_groups": bool(auxiliary_groups),
        "automatic_secret_selection": False,
        "attacks": 500,
        "seeds": seeds,
        "sample_hashes": {},
        "rows": {},
        "omitted_tail_mass": None,
        "ci": _not_estimated_ci(),
        "diagnostic_type": "empirical_attack_diagnostic",
        "formal_privacy_guarantee": False,
    }
    if not explicit:
        return {
            **unavailable,
            "reason": "explicit_secret_and_auxiliary_groups_required",
        }
    if not dependency_available:
        return {**unavailable, "reason": "anonymeter_dependency_unavailable"}
    if real_train is None or real_holdout is None or synthetic is None:
        return {**unavailable, "reason": "anonymeter_tables_unavailable"}

    train_source = Table.from_data(real_train)
    holdout_source = Table.from_data(real_holdout)
    synth_source = Table.from_data(synthetic)
    if any(
        table.column_names != train_source.column_names for table in (holdout_source, synth_source)
    ):
        raise ValueError("Anonymeter datasets must have identical column order")
    secret_columns = [column for group in secret_groups or () for column in group]
    auxiliary = [list(group) for group in auxiliary_groups or ()]
    configured_columns = secret_columns + [column for group in auxiliary for column in group]
    missing = sorted(set(configured_columns) - set(train_source.column_names))
    if missing:
        raise ValueError(f"Anonymeter configured columns do not exist: {missing}")

    train_sample, train_meta = deterministic_sample(
        train_source, GOWER_SAMPLE_ROWS, seed, "anonymeter-real-train"
    )
    holdout_sample, holdout_meta = deterministic_sample(
        holdout_source, GOWER_SAMPLE_ROWS, seed, "anonymeter-holdout"
    )
    synth_sample, synth_meta = deterministic_sample(
        synth_source, GOWER_SAMPLE_ROWS, seed, "anonymeter-synthetic"
    )
    attacks = min(
        500,
        train_sample.row_count,
        holdout_sample.row_count,
        synth_sample.row_count,
    )
    if attacks < 1:
        return {**unavailable, "reason": "anonymeter_requires_nonempty_tables"}

    import pandas as pd
    from anonymeter.evaluators import (
        InferenceEvaluator,
        LinkabilityEvaluator,
        SinglingOutEvaluator,
    )

    def frame(table: Table) -> Any:
        return pd.DataFrame({name: values.tolist() for name, values in table.columns.items()})

    def evaluation_result(evaluator: Any) -> dict[str, Any]:
        results = evaluator.results(confidence_level=0.95)

        def rate(value: Any) -> dict[str, Any] | None:
            if value is None:
                return None
            return {
                "value": float(value.value),
                "ci": {
                    "level": 0.95,
                    "method": "wilson",
                    "low": max(0.0, float(value.value - value.error)),
                    "high": min(1.0, float(value.value + value.error)),
                },
            }

        risk = evaluator.risk(confidence_level=0.95)
        return {
            "main": rate(results.attack_rate),
            "baseline": rate(results.baseline_rate),
            "control": rate(results.control_rate),
            "excess_risk": {
                "value": float(risk.value),
                "ci": {
                    "level": 0.95,
                    "method": "wilson_propagated",
                    "low": float(risk.ci[0]),
                    "high": float(risk.ci[1]),
                },
            },
        }

    original = frame(train_sample)
    control = frame(holdout_sample)
    generated = frame(synth_sample)
    run_results: list[dict[str, Any]] = []
    try:
        for run_seed in seeds:
            np.random.seed(run_seed)
            singling = SinglingOutEvaluator(
                ori=original,
                syn=generated,
                control=control,
                n_attacks=attacks,
                n_cols=min(3, len(original.columns)),
                seed=run_seed,
            )
            singling.evaluate()
            run: dict[str, Any] = {
                "seed": run_seed,
                "singling_out": evaluation_result(singling),
                "linkability": None,
                "inference": {},
            }
            if len(auxiliary) >= 2:
                linkability = LinkabilityEvaluator(
                    ori=original,
                    syn=generated,
                    control=control,
                    aux_cols=(auxiliary[0], auxiliary[1]),
                    n_attacks=attacks,
                )
                linkability.evaluate(n_jobs=1)
                run["linkability"] = evaluation_result(linkability)
            flattened_auxiliary = list(
                dict.fromkeys(column for group in auxiliary for column in group)
            )
            for secret in secret_columns:
                inference_auxiliary = [column for column in flattened_auxiliary if column != secret]
                if not inference_auxiliary:
                    continue
                inference = InferenceEvaluator(
                    ori=original,
                    syn=generated,
                    control=control,
                    aux_cols=inference_auxiliary,
                    secret=secret,
                    n_attacks=attacks,
                )
                inference.evaluate(n_jobs=1)
                run["inference"][secret] = evaluation_result(inference)
            run_results.append(run)
    except Exception as error:
        return {
            **unavailable,
            "reason": "anonymeter_runtime_failed",
            "error_type": type(error).__name__,
            "sample_hashes": {
                "real_train": train_meta["sample_hash"],
                "real_holdout": holdout_meta["sample_hash"],
                "synthetic": synth_meta["sample_hash"],
            },
            "rows": {
                "real_train": train_sample.row_count,
                "real_holdout": holdout_sample.row_count,
                "synthetic": synth_sample.row_count,
            },
        }
    return {
        "applicable": True,
        "dependency_available": True,
        "explicit_secret_groups": True,
        "explicit_auxiliary_groups": True,
        "automatic_secret_selection": False,
        "attacks": attacks,
        "seeds": seeds,
        "sample_hashes": {
            "real_train": train_meta["sample_hash"],
            "real_holdout": holdout_meta["sample_hash"],
            "synthetic": synth_meta["sample_hash"],
        },
        "rows": {
            "real_train": train_sample.row_count,
            "real_holdout": holdout_sample.row_count,
            "synthetic": synth_sample.row_count,
        },
        "results_by_seed": run_results,
        "omitted_tail_mass": None,
        "ci": {
            "level": 0.95,
            "method": "wilson",
        },
        "diagnostic_type": "empirical_attack_diagnostic",
        "formal_privacy_guarantee": False,
    }


def evaluate_advanced(
    real_train_eval: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    real_holdout: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    synthetic: Table | Mapping[str, Sequence[Any]] | Sequence[Mapping[str, Any]],
    column_types: Mapping[str, str],
    *,
    seed: int = 0,
    mode: str = "utility",
    report_scope: str = "internal",
    target: str | None = None,
    task: str | None = None,
    secret_groups: Sequence[Sequence[str]] | None = None,
    auxiliary_groups: Sequence[Sequence[str]] | None = None,
    public_categories: Mapping[str, Sequence[Any]] | None = None,
    public_bins: Mapping[str, Sequence[float]] | None = None,
    sections: Sequence[str] | None = None,
) -> dict[str, Any]:
    if mode not in {"utility", "differential_privacy"}:
        raise ValueError("mode must be utility or differential_privacy")
    if report_scope not in {"internal", "release"}:
        raise ValueError("report_scope must be internal or release")
    requested = set(sections or ("pairwise", "c2st", "downstream_utility", "empirical_privacy"))
    dp_release = mode == "differential_privacy" and report_scope == "release"
    output: dict[str, Any] = {
        "evaluation_config_version": EVALUATION_CONFIG_VERSION,
        "mode": mode,
        "report_scope": report_scope,
        "universal_score": None,
        "seed": seed,
    }
    if "pairwise" in requested:
        output["pairwise"] = pairwise_metrics(
            real_train_eval,
            synthetic,
            column_types,
            real_holdout=real_holdout,
            seed=derive_seed(seed, "pairwise"),
            dp_release=dp_release,
            public_categories=public_categories,
            public_bins=public_bins,
        )
    if "c2st" in requested:
        output["c2st"] = c2st_metrics(
            real_train_eval,
            real_holdout,
            synthetic,
            column_types,
            seed=derive_seed(seed, "c2st"),
        )
    if "downstream_utility" in requested:
        output["downstream_utility"] = downstream_utility_metrics(
            real_train_eval,
            real_holdout,
            synthetic,
            column_types,
            target=target,
            task=task,
            seed=derive_seed(seed, "downstream-utility"),
        )
    if "empirical_privacy" in requested and report_scope == "internal":
        output["empirical_privacy"] = {
            "release_safe": False,
            "formal_privacy_guarantee": False,
            "gower": gower_privacy_metrics(
                real_train_eval,
                real_holdout,
                synthetic,
                column_types,
                seed=derive_seed(seed, "gower"),
            ),
            "anonymeter": anonymeter_metrics(
                real_train_eval,
                real_holdout,
                synthetic,
                secret_groups=secret_groups,
                auxiliary_groups=auxiliary_groups,
                seed=derive_seed(seed, "anonymeter"),
            ),
        }
    return output
