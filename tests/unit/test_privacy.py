from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest

from sts.domain import DomainError, ErrorCode
from sts.privacy import (
    MAX_ESTIMATED_STATE_BYTES,
    DiscreteCodebook,
    LedgerRunState,
    PrivacyLedger,
    PrivateFitRngPolicy,
    PublicFitSamplingPredicate,
    admit_mst_domain,
    create_private_fit_rng,
    estimate_mst_state,
    load_dp_availability,
    validate_public_metadata,
)

WHEEL_SHA256 = "fbd71d26caa51733cf1d8382f140faa755d7157e7d471191fee2c4a862a2f51b"


def public_manifest_payload() -> dict[str, object]:
    return {
        "version": "1.0",
        "provenance": {
            "provenance": "public",
            "issuer": "National public codebook authority",
            "description": "Published sex and age domains",
            "source_sha256": "a" * 64,
            "user_attested_public": True,
            "attested_by": "curator@example.test",
        },
        "epsilon_preprocess": 0,
        "columns": [
            {
                "encoding": "categories",
                "name": "sex",
                "kind": "categorical",
                "categories": ["F", "M"],
                "nullable": True,
                "missing_sentinel": "__PUBLIC_MISSING__",
            },
            {
                "encoding": "bins",
                "name": "age",
                "kind": "integer",
                "bins": [0, 18, 65, 120],
                "within_bin": {"kind": "uniform"},
                "nullable": False,
            },
            {
                "encoding": "bins",
                "name": "joined",
                "kind": "date",
                "bins": ["2020-01-01", "2022-01-01", "2025-01-01"],
                "within_bin": {"kind": "uniform"},
                "nullable": False,
            },
        ],
        "public_rules_sha256": "b" * 64,
    }


def test_private_or_unattested_metadata_is_rejected() -> None:
    private = public_manifest_payload()
    private["provenance"] = {
        **private["provenance"],
        "provenance": "private_inferred",
    }
    with pytest.raises(DomainError) as private_error:
        validate_public_metadata(private)
    assert private_error.value.code is ErrorCode.DP_METADATA_NOT_PUBLIC

    unattested = public_manifest_payload()
    unattested["provenance"] = {
        **unattested["provenance"],
        "user_attested_public": False,
    }
    with pytest.raises(DomainError) as attestation_error:
        validate_public_metadata(unattested)
    assert attestation_error.value.code is ErrorCode.DP_METADATA_NOT_PUBLIC

    private_discovery = public_manifest_payload()
    private_discovery["private_categories"] = ["source-derived"]
    with pytest.raises(DomainError):
        validate_public_metadata(private_discovery)


def test_public_discrete_codebook_uses_declared_categories_bins_and_missing_state() -> (
    None
):
    manifest = validate_public_metadata(public_manifest_payload())
    codebook = DiscreteCodebook(manifest)
    assert codebook.domain == {"sex": 3, "age": 3, "joined": 2}
    assert codebook.encode_row(
        {"sex": None, "age": 120, "joined": date(2021, 6, 1)}
    ) == {"sex": 2, "age": 2, "joined": 0}
    assert codebook.encode_value("sex", "F") == 0
    with pytest.raises(DomainError):
        codebook.encode_value("sex", "source-only-category")
    with pytest.raises(DomainError):
        codebook.encode_value("age", 121)

    encoded = {"sex": 0, "age": 1, "joined": 1}
    first = codebook.decode_row(encoded, sampling_seed=927)
    second = codebook.decode_row(encoded, sampling_seed=927)
    assert first == second
    assert first["sex"] == "F"
    assert 18 <= first["age"] <= 64
    assert date(2022, 1, 1) <= first["joined"] <= date(2025, 1, 1)
    assert (
        codebook.decode_row({"sex": 2, "age": 0, "joined": 0}, sampling_seed=1)["sex"]
        is None
    )


def test_mst_modeled_column_and_state_boundaries() -> None:
    accepted = admit_mst_domain([256] * 32, projected_worker_rss_bytes=1024)
    assert accepted.estimate.modeled_columns == 32
    assert accepted.estimate.domain_sizes == (256,) * 32

    with pytest.raises(DomainError) as columns:
        admit_mst_domain([2] * 33, projected_worker_rss_bytes=1024)
    assert columns.value.code is ErrorCode.DP_DOMAIN_TOO_LARGE
    assert "modeled_columns" in columns.value.problem.context["failed_gates"]

    with pytest.raises(DomainError) as states:
        admit_mst_domain([257], projected_worker_rss_bytes=1024)
    assert states.value.code is ErrorCode.DP_DOMAIN_TOO_LARGE
    assert "states_per_column" in states.value.problem.context["failed_gates"]


def test_mst_largest_pair_state_and_worker_rss_gates_are_independent() -> None:
    pair_estimate = estimate_mst_state([1_001, 1_000])
    assert pair_estimate.largest_pair_cells == 1_001_000
    with pytest.raises(DomainError) as pair:
        admit_mst_domain([1_001, 1_000], projected_worker_rss_bytes=0)
    assert "largest_pair_cells" in pair.value.problem.context["failed_gates"]

    state_estimate = estimate_mst_state([1_000] * 32)
    assert state_estimate.largest_pair_cells == 1_000_000
    assert state_estimate.estimated_state_bytes > MAX_ESTIMATED_STATE_BYTES
    with pytest.raises(DomainError) as state:
        admit_mst_domain([1_000] * 32, projected_worker_rss_bytes=0)
    assert "estimated_state_bytes" in state.value.problem.context["failed_gates"]

    with pytest.raises(DomainError) as rss:
        admit_mst_domain(
            [2, 2],
            projected_worker_rss_bytes=10_001,
            worker_rss_limit_bytes=10_000,
        )
    assert rss.value.code is ErrorCode.RESOURCE_LIMIT


def test_public_fit_sampling_is_deterministic_rowwise_one_stable() -> None:
    predicate = PublicFitSamplingPredicate(Decimal("0.5"), b"public-key-00001")
    rows = (
        {"a": 1, "b": "x"},
        {"a": 2, "b": "y"},
        {"a": 3, "b": "z"},
    )
    reordered_keys = {"b": "x", "a": 1}
    assert predicate.selected(rows[0]) == predicate.selected(reordered_keys)
    assert [predicate.selected(row) for row in rows] == [
        predicate.selected(row) for row in rows
    ]

    selected_before = predicate.select_rows(rows, maximum_selected_rows=3)
    added = {"a": 4, "b": "new"}
    selected_after = predicate.select_rows((*rows, added), maximum_selected_rows=4)
    assert selected_after[: len(selected_before)] == selected_before
    assert len(selected_after) - len(selected_before) in {0, 1}
    contract = predicate.public_contract()
    assert contract["stability"] == "rowwise_1_stable"
    assert contract["amplification_claimed"] is False
    assert contract["truncation_allowed"] is False


def test_fit_selection_over_capacity_rejects_without_truncation_or_amplification() -> (
    None
):
    predicate = PublicFitSamplingPredicate(Decimal("1"), b"public-key-00001")
    with pytest.raises(DomainError) as raised:
        predicate.select_rows(({"x": 1}, {"x": 2}), maximum_selected_rows=1)
    assert raised.value.code is ErrorCode.RESOURCE_LIMIT
    assert raised.value.problem.context == {
        "maximum_selected_rows": 1,
        "truncated": False,
        "amplification_claimed": False,
    }


def test_private_fit_rng_uses_os_csprng_and_discloses_only_policy(monkeypatch) -> None:
    calls: list[int] = []
    secret_entropy = bytes(range(32))

    def fake_token_bytes(length: int) -> bytes:
        calls.append(length)
        return secret_entropy

    monkeypatch.setattr("sts.privacy.rng.secrets.token_bytes", fake_token_bytes)
    handle = create_private_fit_rng()
    assert calls == [32]
    public = handle.public_record()
    assert set(public) == {
        "version",
        "entropy_source",
        "rng_implementation",
        "commitment_sha256",
    }
    rendered = json.dumps(public, sort_keys=True)
    assert secret_entropy.hex() not in rendered
    assert "private_material=<redacted>" in repr(handle)
    assert secret_entropy.hex() not in repr(handle)
    with pytest.raises(TypeError):
        handle.__reduce__()


def _reserve(
    ledger: PrivacyLedger,
    scope_id,
    *,
    epsilon: str,
    delta: str,
    internal_private: dict[str, object] | None = None,
):
    return ledger.reserve_run(
        scope_id,
        uuid4(),
        epsilon_model=epsilon,
        delta=delta,
        package_version="0.1.9",
        wheel_sha256=WHEEL_SHA256,
        public_metadata_hashes=("c" * 64, "d" * 64),
        rng_policy=PrivateFitRngPolicy(commitment_sha256="e" * 64),
        public_target_count=10_000,
        public_target_count_provenance="public request parameter",
        conversion="dpmm MST cDP to (epsilon, delta)",
        public_rule_postprocessing=(
            {"kind": "allowed_values", "provenance": "public"},
        ),
        limitations=("row-level DP; no amplification claimed",),
        internal_private=internal_private,
    )


@pytest.mark.parametrize("reason", ["crash", "cancel"])
def test_pre_private_read_crash_or_cancel_is_unspent(tmp_path, reason: str) -> None:
    with PrivacyLedger(tmp_path / f"ledger-{reason}.sqlite3") as ledger:
        scope = ledger.create_privacy_scope("1" * 64)
        run = _reserve(ledger, scope.privacy_scope_id, epsilon="3", delta="0.000001")
        aborted = ledger.record_abort(run.run_id, reason=reason)
        assert aborted.state is LedgerRunState.ABORTED_BEFORE_PRIVATE_ACCESS
        assert ledger.composition(scope.privacy_scope_id).spent_runs == 0
        with pytest.raises(DomainError):
            ledger.mark_private_access(run.run_id)


@pytest.mark.parametrize("reason", ["crash", "cancel"])
def test_post_private_read_crash_or_cancel_remains_spent(tmp_path, reason: str) -> None:
    with PrivacyLedger(tmp_path / f"ledger-{reason}.sqlite3") as ledger:
        scope = ledger.create_privacy_scope("2" * 64)
        run = _reserve(ledger, scope.privacy_scope_id, epsilon="3", delta="0.000001")
        spent = ledger.mark_private_access(run.run_id)
        assert spent.state is LedgerRunState.SPENT_NOT_RELEASED
        aborted = ledger.record_abort(run.run_id, reason=reason)
        assert aborted.state is LedgerRunState.SPENT_NOT_RELEASED
        assert ledger.composition(scope.privacy_scope_id).epsilon_total == Decimal("3")


def test_basic_sequential_epsilon_delta_composition_known_vector(tmp_path) -> None:
    with PrivacyLedger(tmp_path / "ledger.sqlite3") as ledger:
        scope = ledger.create_privacy_scope("3" * 64)
        aborted = _reserve(ledger, scope.privacy_scope_id, epsilon="9", delta="0.1")
        ledger.record_abort(
            aborted.run_id, reason="admission failure before source open"
        )
        first = _reserve(
            ledger, scope.privacy_scope_id, epsilon="1.25", delta="0.000001"
        )
        second = _reserve(
            ledger, scope.privacy_scope_id, epsilon="2.75", delta="0.000002"
        )
        ledger.mark_private_access(first.run_id)
        ledger.mark_private_access(second.run_id)

        composition = ledger.composition(scope.privacy_scope_id)
        assert composition.accountant == "basic_sequential"
        assert composition.epsilon_preprocess == 0
        assert composition.epsilon_total == Decimal("4.00")
        assert composition.delta_total == Decimal("0.000003")
        assert composition.spent_runs == 2


def test_same_model_repeated_release_increments_count_without_new_spend(
    tmp_path,
) -> None:
    with PrivacyLedger(tmp_path / "ledger.sqlite3") as ledger:
        scope = ledger.create_privacy_scope("4" * 64)
        run = _reserve(ledger, scope.privacy_scope_id, epsilon="3", delta="0.000001")
        ledger.mark_private_access(run.run_id)
        model_id = uuid4()
        release_id = uuid4()
        first = ledger.record_release(run.run_id, model_id, release_id=release_id)
        idempotent = ledger.record_release(run.run_id, model_id, release_id=release_id)
        second = ledger.record_release(run.run_id, model_id)
        assert first.release_count == 1
        assert idempotent.release_count == 1
        assert second.release_count == 2
        assert second.state is LedgerRunState.RELEASED
        assert ledger.composition(scope.privacy_scope_id).spent_runs == 1
        assert ledger.release_projection(run.run_id).release_count == 2


def test_release_projection_is_strict_whitelist_without_private_fields(
    tmp_path,
) -> None:
    private = {
        "source_count": 987_654,
        "exact_private_domain": {"secret": ["a", "b"]},
        "holdout_fidelity": 0.9,
        "attack_result": {"dcr": 0.01},
        "raw_example": "private row",
        "fit_timing_seconds": 12.3,
        "peak_rss_bytes": 999,
        "raw_content_hmac": "private hmac",
    }
    with PrivacyLedger(tmp_path / "ledger.sqlite3") as ledger:
        scope = ledger.create_privacy_scope("5" * 64)
        run = _reserve(
            ledger,
            scope.privacy_scope_id,
            epsilon="3",
            delta="0.001",
            internal_private=private,
        )
        ledger.mark_private_access(run.run_id)
        model_id = uuid4()
        ledger.record_release(run.run_id, model_id)
        projection = ledger.release_projection(run.run_id).model_dump(mode="json")
        rendered = json.dumps(projection, sort_keys=True)
        for forbidden in (
            "dataset_manifest",
            "source_count",
            "exact_private_domain",
            "holdout",
            "attack",
            "raw_example",
            "fit_timing",
            "peak_rss",
            "raw_content_hmac",
            "job_id",
            "package_version",
            'public_target_count"',
        ):
            assert forbidden not in rendered
        assert projection["privacy_scope_id"] == str(scope.privacy_scope_id)
        assert projection["model_id"] == str(model_id)
        assert projection["limitations"] == [
            "row-level DP; no amplification claimed",
            "delta_exceeds_inverse_public_target_count_advisory",
        ]
        internal = ledger.internal_run_details(run.run_id)
        assert internal["dataset_manifest_sha256"] == "5" * 64
        assert internal["internal_private"] == private


def test_private_fit_seed_or_rng_state_cannot_enter_ledger(tmp_path) -> None:
    with PrivacyLedger(tmp_path / "ledger.sqlite3") as ledger:
        scope = ledger.create_privacy_scope("6" * 64)
        with pytest.raises(ValueError, match="must never enter the ledger"):
            _reserve(
                ledger,
                scope.privacy_scope_id,
                epsilon="3",
                delta="0.000001",
                internal_private={"nested": {"fit_seed": 12345}},
            )


def test_availability_uses_phase_zero_result_and_keeps_mst_and_aim_disabled(
    tmp_path,
) -> None:
    availability = load_dp_availability()
    assert availability.formal_dp_enabled is False
    assert availability.aim_enabled is False
    assert availability.probe_status == "failed"
    assert availability.failed_gates == ("checkpoint_schema_and_secret_audit",)
    assert availability.failure_reasons == ("checkpoint_schema_and_secret_audit",)

    inconsistent = tmp_path / "probe.json"
    inconsistent.write_text(
        json.dumps(
            {
                "status": "passed",
                "formal_dp_enabled": True,
                "formal_dp_gate": {},
                "failure_reasons": [],
                "aim": {"enabled": True, "equivalent_gates_executed": True},
            }
        ),
        encoding="utf-8",
    )
    fail_closed = load_dp_availability(inconsistent)
    assert fail_closed.formal_dp_enabled is False
    assert fail_closed.aim_enabled is False
    assert set(fail_closed.failed_gates) == {
        "environment",
        "fit",
        "persist",
        "fresh_process_repeated_sample",
        "checkpoint_schema_and_secret_audit",
        "public_false_source_audit",
        "add_remove_accounting",
        "conservative_state_estimates",
    }
