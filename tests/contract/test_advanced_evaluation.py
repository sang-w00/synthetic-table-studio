from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path

import numpy as np
import pytest

from eval_worker.evaluation import (
    PAIRWISE_MAX_PAIRS,
    anonymeter_metrics,
    c2st_metrics,
    downstream_utility_metrics,
    evaluate_advanced,
    exact_gower_nearest,
    gower_privacy_metrics,
    pairwise_metrics,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _c2st_fixture() -> tuple[
    dict[str, np.ndarray], dict[str, np.ndarray], dict[str, np.ndarray]
]:
    rows = 120
    real = {
        "number": np.linspace(-1.0, 1.0, rows),
        "category": np.asarray(["a", "b"] * (rows // 2)),
        "ignored_text": np.asarray([f"private-{index}" for index in range(rows)]),
    }
    holdout = {
        "number": np.linspace(10_000.0, 10_100.0, rows),
        "category": np.asarray(["holdout-only"] * rows),
        "ignored_text": np.asarray([f"holdout-{index}" for index in range(rows)]),
    }
    synthetic = {
        "number": np.linspace(-0.8, 1.2, rows),
        "category": np.asarray(["a", "b"] * (rows // 2)),
        "ignored_text": np.asarray([f"synthetic-{index}" for index in range(rows)]),
    }
    return real, holdout, synthetic


def test_pairwise_caps_pairs_and_fits_grouping_on_real_train_only() -> None:
    values = np.asarray([f"value-{index:02d}" for index in range(55)])
    real = {
        "left": values,
        "right": values[::-1],
        "number": np.arange(55, dtype=float),
    }
    synthetic = {
        "left": np.roll(values, 1),
        "right": np.roll(values[::-1], 2),
        "number": np.arange(55, dtype=float)[::-1],
    }
    result = pairwise_metrics(
        real,
        synthetic,
        {"left": "categorical", "right": "categorical", "number": "float"},
        seed=17,
        max_pairs=2,
    )

    assert result["pair_cap"] == PAIRWISE_MAX_PAIRS == 2_415
    assert result["pairs_considered"] == 2
    assert result["pairs_omitted"] == 1
    categorical = result["pairs"][0]["synthetic_difference"]
    assert categorical["grouping"]["source"] == "real_train_eval"
    assert categorical["grouping"]["top_values_per_axis"] == 50
    assert categorical["grouping"]["contingency_cells_cap"] == 2_601
    assert categorical["omitted_tail_mass"]["left"] == pytest.approx(5 / 55)
    assert len(categorical["grouping"]["left_retained"]) == 50
    with pytest.raises(ValueError, match="max_pairs"):
        pairwise_metrics(
            real,
            synthetic,
            {"left": "categorical", "right": "categorical", "number": "float"},
            max_pairs=2_416,
        )


def test_c2st_preprocessing_never_fits_holdout_or_synthetic_and_settings_are_fixed() -> (
    None
):
    real, holdout, synthetic = _c2st_fixture()
    result = c2st_metrics(
        real,
        holdout,
        synthetic,
        {"number": "float", "category": "categorical", "ignored_text": "text"},
        seed=1234,
    )

    assert result["applicable"] is True
    assert result["preprocessing"]["fit_dataset"] == "real_train_eval"
    assert result["preprocessing"]["holdout_used_for_fit"] is False
    assert result["preprocessing"]["synthetic_used_for_fit"] is False
    assert abs(result["preprocessing"]["linear"]["numeric_medians"][0]) < 0.02
    assert result["preprocessing"]["nonlinear"]["category_counts"] == {"category": 2}
    assert len(result["seeds"]) == 5

    comparison = result["synthetic_vs_untouched_holdout"]
    assert comparison["linear"]["settings"] == {
        "solver": "saga",
        "C": 1,
        "max_iter": 500,
        "class_weight": "balanced",
        "n_jobs": 1,
    }
    assert comparison["nonlinear"]["settings"] == {
        "max_iter": 200,
        "learning_rate": 0.05,
        "max_leaf_nodes": 31,
        "min_samples_leaf": 50,
        "l2_regularization": 1,
        "categorical_features": [False, True],
    }
    assert comparison["linear"]["ci"]["iterations"] == 500
    assert result["real_train_eval_vs_untouched_holdout_control"]["linear"][
        "applicable"
    ]
    assert set(result["sample_hashes"]) == {
        "preprocessing_real_train_eval",
        "control_real_train_eval",
        "real_holdout",
        "synthetic",
    }


def test_downstream_utility_requires_explicit_target_and_task() -> None:
    real, holdout, synthetic = _c2st_fixture()
    result = downstream_utility_metrics(
        real,
        holdout,
        synthetic,
        {"number": "float", "category": "categorical", "ignored_text": "text"},
        target=None,
        task=None,
        seed=2,
    )
    assert result["applicable"] is False
    assert result["automatic_target_selection"] is False
    assert result["reason"] == "explicit_target_and_task_required"


def test_exact_gower_known_values_and_control_bootstrap_are_deterministic() -> None:
    train = {"number": np.asarray([0.0, 10.0]), "category": np.asarray(["a", "b"])}
    queries = {"number": np.asarray([0.0, 5.0]), "category": np.asarray(["a", "a"])}
    first, second, bounds = exact_gower_nearest(
        train,
        queries,
        {"number": "float", "category": "categorical"},
        block_rows=1,
    )
    assert bounds["number"] == pytest.approx((0.1, 9.9))
    assert first == pytest.approx([0.0, 0.25])
    assert second == pytest.approx([1.0, 0.75])

    real_train = {
        "number": np.asarray([0.0, 2.0, 8.0, 10.0]),
        "category": np.asarray(["a", "a", "b", "b"]),
        "identifier": np.asarray(["id-1", "id-2", "id-3", "id-4"]),
    }
    holdout = {
        "number": np.asarray([1.0, 3.0, 7.0, 9.0]),
        "category": np.asarray(["a", "a", "b", "b"]),
        "identifier": np.asarray(["h-1", "h-2", "h-3", "h-4"]),
    }
    synthetic = {
        "number": np.asarray([0.0, 2.1, 7.9, 10.0]),
        "category": np.asarray(["a", "a", "b", "b"]),
        "identifier": np.asarray(["s-1", "s-2", "s-3", "s-4"]),
    }
    kinds = {"number": "float", "category": "categorical", "identifier": "identifier"}
    first_run = gower_privacy_metrics(real_train, holdout, synthetic, kinds, seed=91)
    second_run = gower_privacy_metrics(real_train, holdout, synthetic, kinds, seed=91)

    assert first_run == second_run
    assert first_run["distance"] == "blockwise_exact_gower"
    assert first_run["columns"] == ["number", "category"]
    assert first_run["formal_privacy_guarantee"] is False
    assert "holdout_to_train_control" in first_run["dcr"]
    assert first_run["dcr"]["synthetic_median_ci"]["iterations"] == 500
    assert first_run["nndr"]["difference_ci"]["iterations"] == 500


def test_anonymeter_never_automatically_selects_secrets() -> None:
    result = anonymeter_metrics(secret_groups=None, auxiliary_groups=None, seed=7)
    assert result["applicable"] is False
    assert result["automatic_secret_selection"] is False
    assert result["reason"] == "explicit_secret_and_auxiliary_groups_required"
    assert result["formal_privacy_guarantee"] is False
    assert len(result["seeds"]) == 3


def test_dp_release_configuration_omits_empirical_privacy() -> None:
    real, holdout, synthetic = _c2st_fixture()
    result = evaluate_advanced(
        real,
        holdout,
        synthetic,
        {"number": "float", "category": "categorical", "ignored_text": "text"},
        seed=3,
        mode="differential_privacy",
        report_scope="release",
        secret_groups=[["number"]],
        auxiliary_groups=[["category"]],
        sections=["empirical_privacy"],
    )
    assert "empirical_privacy" not in result
    assert result["universal_score"] is None


def test_pairwise_worker_operation_publishes_internal_non_release_artifact(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    datasets = workspace / "datasets"
    attempt = workspace / "jobs" / "job-advanced" / "attempt-1"
    datasets.mkdir(parents=True)
    attempt.mkdir(parents=True)
    fixture = {
        "columns": {
            "number": list(range(20)),
            "left": ["a", "b"] * 10,
            "right": ["x", "y", "z", "x"] * 5,
        }
    }
    snapshots: dict[str, dict[str, object]] = {}
    for role in ("real_train_eval", "real_holdout", "synthetic"):
        path = datasets / f"{role}.json"
        path.write_text(json.dumps(fixture), encoding="utf-8")
        snapshots[role] = {
            "path": path.relative_to(workspace).as_posix(),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "size_bytes": path.stat().st_size,
        }
    request = {
        "version": "1.0",
        "request_id": "request-advanced",
        "job_id": "job-advanced",
        "attempt": 1,
        "worker_kind": "eval",
        "operation": "pairwise_evaluation",
        "manifest_snapshot": {
            "version": "1.0",
            "workspace_root": str(workspace),
            "files": snapshots,
        },
        "limits": {
            "evaluation_config": {
                "version": "1.0",
                "seed": 44,
                "mode": "utility",
                "report_scope": "internal",
                "column_types": {
                    "number": "float",
                    "left": "categorical",
                    "right": "categorical",
                },
            }
        },
        "cancellation_path": "jobs/job-advanced/attempt-1/cancel",
    }
    request_path = attempt / "request.json"
    events_path = attempt / "events.jsonl"
    result_path = attempt / "result.json"
    request_path.write_text(json.dumps(request), encoding="utf-8")
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(PROJECT_ROOT / "workers" / "eval" / "src")
    completed = subprocess.run(
        [
            str(PROJECT_ROOT / "workers" / "eval" / ".venv" / "bin" / "python"),
            "-m",
            "eval_worker",
            "run",
            "--request",
            str(request_path),
            "--events",
            str(events_path),
            "--result",
            str(result_path),
        ],
        capture_output=True,
        env=environment,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr.decode()
    result = json.loads(result_path.read_bytes())
    assert result["status"] == "success"
    artifact = result["artifacts"][0]
    assert artifact["kind"] == "internal_diagnostic_report_json"
    assert artifact["release_safe"] is False
    assert artifact["contains_private_source_information"] is True
    evaluation = json.loads((workspace / artifact["path"]).read_bytes())
    assert set(evaluation) >= {"evaluation_config_version", "pairwise"}
    assert set(evaluation).isdisjoint(
        {"c2st", "downstream_utility", "empirical_privacy"}
    )
    assert [
        json.loads(line)["sequence"] for line in events_path.read_text().splitlines()
    ] == [
        1,
        2,
        3,
    ]
