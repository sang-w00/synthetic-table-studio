from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest

from sts.domain import (
    ColumnKind,
    ColumnRole,
    ColumnSchema,
    DatasetManifest,
    DatasetState,
    ManifestFile,
)
from sts.privacy import load_dp_mechanism_provenance
from sts.storage import CatalogRepository, WorkspaceLayout
from sts.storage.repository import LedgerRunState


def _repository(tmp_path):
    layout = WorkspaceLayout(tmp_path / "workspace")
    return CatalogRepository.open_workspace(layout)


def _job(repository, dataset_manifest_sha: str, key: str):
    dataset_id = uuid4()
    manifest = DatasetManifest(
        dataset_id=dataset_id,
        source=ManifestFile(
            relative_path=f"datasets/{dataset_id}/source.csv",
            sha256="0" * 64,
            size_bytes=0,
        ),
        schema_version="schema-1",
        rules_version="rules-1",
        columns=(
            ColumnSchema(
                name="age",
                kind=ColumnKind.INTEGER,
                nullable=False,
                role=ColumnRole.MODEL,
            ),
        ),
    )
    dataset = repository.create_dataset(manifest, state=DatasetState.NORMALIZED)
    return repository.create_job(
        {
            "version": "1.0",
            "dataset_id": str(dataset_id),
            "dataset_manifest_sha": dataset.manifest_sha256,
            "schema_version": "schema-1",
            "rules_version": "rules-1",
            "mode": "utility",
            "synthesizer": "tabular_argn",
            "output_rows": 4,
            "output_formats": ["parquet"],
            "resource_profile": "m4-default",
            "evaluation_config_version": "1.0",
            "training": {
                "max_rows": 4,
                "max_epochs": 1,
                "max_minutes": 1,
                "model_size": "tiny",
                "device": "cpu",
            },
        },
        idempotency_key=key,
    )


def _reserve(repository, scope_id, job, epsilon: str, delta: str):
    return repository.reserve_ledger_run(
        scope_id,
        job.job_id,
        epsilon_model=Decimal(epsilon),
        delta=Decimal(delta),
        record={
            "version": "1.0",
            "mechanism": "MST",
            "epsilon_model": epsilon,
            "delta": delta,
        },
    )


def test_scope_composition_sums_every_run_that_touched_the_private_source(
    tmp_path,
) -> None:
    repository = _repository(tmp_path)
    digest = "a" * 64
    scope = repository.create_privacy_scope(digest)

    empty = repository.ledger_scope_composition(scope.privacy_scope_id)
    assert empty["epsilon_total"] == "0"
    assert empty["spent_runs"] == 0
    assert empty["release_count"] == 0

    # Reserved but never touching the source: no privacy loss, no contribution.
    reserved = _reserve(
        repository,
        scope.privacy_scope_id,
        _job(repository, digest, "k0"),
        "1",
        "0.000001",
    )
    repository.transition_ledger_run(
        reserved.run_id, LedgerRunState.ABORTED_BEFORE_PRIVATE_ACCESS
    )
    assert (
        repository.ledger_scope_composition(scope.privacy_scope_id)["epsilon_total"]
        == "0"
    )

    # Spent but withheld: the loss happened, so it still composes.
    spent = _reserve(
        repository,
        scope.privacy_scope_id,
        _job(repository, digest, "k1"),
        "1.5",
        "0.000001",
    )
    repository.transition_ledger_run(spent.run_id, LedgerRunState.SPENT_NOT_RELEASED)

    first = _reserve(
        repository,
        scope.privacy_scope_id,
        _job(repository, digest, "k2"),
        "3",
        "0.000002",
    )
    repository.transition_ledger_run(first.run_id, LedgerRunState.SPENT_NOT_RELEASED)
    repository.transition_ledger_run(
        first.run_id, LedgerRunState.RELEASED, model_id=uuid4()
    )

    second = _reserve(
        repository,
        scope.privacy_scope_id,
        _job(repository, digest, "k3"),
        "2",
        "0.000004",
    )
    repository.transition_ledger_run(second.run_id, LedgerRunState.SPENT_NOT_RELEASED)
    repository.transition_ledger_run(
        second.run_id, LedgerRunState.RELEASED, model_id=uuid4()
    )

    composition = repository.ledger_scope_composition(scope.privacy_scope_id)
    assert composition["accountant"] == "basic_sequential"
    assert Decimal(composition["epsilon_total"]) == Decimal("6.5")
    assert Decimal(composition["delta_total"]) == Decimal("0.000007")
    assert composition["spent_runs"] == 3
    assert composition["release_count"] == 2


def test_composition_is_isolated_per_scope(tmp_path) -> None:
    repository = _repository(tmp_path)
    left = repository.create_privacy_scope("b" * 64)
    right = repository.create_privacy_scope("c" * 64)
    run = _reserve(
        repository,
        left.privacy_scope_id,
        _job(repository, "b" * 64, "s1"),
        "5",
        "0.00001",
    )
    repository.transition_ledger_run(run.run_id, LedgerRunState.SPENT_NOT_RELEASED)

    assert Decimal(
        repository.ledger_scope_composition(left.privacy_scope_id)["epsilon_total"]
    ) == Decimal("5")
    assert Decimal(
        repository.ledger_scope_composition(right.privacy_scope_id)["epsilon_total"]
    ) == Decimal("0")


def test_mechanism_provenance_reads_the_probe_and_never_guesses(tmp_path) -> None:
    provenance = load_dp_mechanism_provenance()
    assert provenance.accountant == "basic_sequential"
    assert provenance.wheel_sha256 is not None and len(provenance.wheel_sha256) == 64
    assert provenance.conversion is not None and "zcdp_rho" in provenance.conversion

    missing = load_dp_mechanism_provenance(tmp_path / "absent.json")
    assert missing.wheel_sha256 is None
    assert missing.conversion is None
    assert missing.package_version is None


@pytest.mark.parametrize(
    "payload", ['{"environment": {"dpmm_wheel_sha256": "short"}}', "not json"]
)
def test_mechanism_provenance_drops_unusable_probe_values(
    tmp_path, payload: str
) -> None:
    path = tmp_path / "probe.json"
    path.write_text(payload, encoding="utf-8")
    assert load_dp_mechanism_provenance(path).wheel_sha256 is None
