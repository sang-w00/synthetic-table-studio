from __future__ import annotations

import dataclasses
import hashlib
import json
import os
from enum import Enum
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, Request, Response, status
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from sts.domain import (
    ColumnSchema,
    DatasetManifest,
    DatasetState,
    DomainError,
    ErrorCode,
    ManifestFile,
    canonical_json_bytes,
)
from sts.ingest.normalize import normalize_to_parquet, raw_columns, validate_schema
from sts.profile import DatasetProfile, profile_parquet
from sts.rules import RuleSpec, compile_rules
from sts.storage import CatalogRepository, WorkspaceLayout
from sts.storage.repository import OwnerType

from .events import dataset_event_response

ZERO_SHA256 = "0" * 64


class UploadCreateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    filename: str = Field(min_length=1)
    size_bytes: int = Field(ge=0)
    source_format: Literal["csv", "xlsx"]


class UploadCompleteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sha256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")


class ParseOptionsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    encoding: Literal["utf-8", "utf-8-sig", "cp949", "euc-kr"]
    delimiter: str = Field(min_length=1, max_length=1)
    quotechar: str | None = Field(default='"', min_length=1, max_length=1)
    escapechar: str | None = Field(default=None, min_length=1, max_length=1)
    has_header: bool = True
    malformed: Literal["fail", "skip"] = "fail"


class SheetSelectionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)


class SchemaRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    columns: tuple[ColumnSchema, ...]


class RulesRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rules: tuple[RuleSpec, ...]


class RetryResponse(BaseModel):
    dataset_id: UUID
    state: DatasetState
    attempt: int


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _jsonable(getattr(value, field.name)) for field in dataclasses.fields(value)
        }
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _atomic_json(path: Path, value: Any) -> None:
    payload = json.dumps(
        _jsonable(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.part")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class DatasetService:
    """Synchronous dataset control-plane service used by the FastAPI routes.

    CSV and XLSX readers are imported only inside their operation boundary so this
    service can be constructed while ingest adapters are independently installed.
    """

    def __init__(
        self,
        repository: CatalogRepository,
        workspace: WorkspaceLayout,
        *,
        upload_manager: Any | None = None,
        duckdb_memory_limit: str = "1GB",
    ) -> None:
        self.repository = repository
        self.workspace = workspace
        self.workspace.initialize()
        if upload_manager is None:
            from sts.ingest.upload import UploadManager

            upload_manager = UploadManager(workspace)
        self.uploads = upload_manager
        self.duckdb_memory_limit = duckdb_memory_limit

    def _directory(self, dataset_id: UUID | str) -> Path:
        return self.workspace.dataset_dir(dataset_id, create=True)

    def _control_path(self, dataset_id: UUID | str) -> Path:
        return self._directory(dataset_id) / ".dataset-api.json"

    def _control(self, dataset_id: UUID | str) -> dict[str, Any]:
        path = self._control_path(dataset_id)
        try:
            value = json.loads(path.read_bytes())
        except (FileNotFoundError, json.JSONDecodeError) as exc:
            raise DomainError(
                ErrorCode.INVALID_STATE, "dataset control metadata is unavailable"
            ) from exc
        if not isinstance(value, dict) or value.get("version") != "1.0":
            raise DomainError(ErrorCode.INVALID_STATE, "dataset control metadata is invalid")
        return value

    def _save_control(self, dataset_id: UUID | str, control: dict[str, Any]) -> None:
        _atomic_json(self._control_path(dataset_id), control)

    def _emit(
        self,
        dataset_id: UUID | str,
        *,
        stage: str,
        state: DatasetState,
        completed: int = 1,
        total: int = 1,
        terminal: bool = False,
        code: str | None = None,
    ) -> None:
        self.repository.append_event(
            OwnerType.DATASET,
            dataset_id,
            {
                "version": "1.0",
                "stage": stage,
                "state": state.value,
                "completed": completed,
                "total": total,
                "unit": "steps",
                "message_code": code or f"DATASET_{state.value.upper()}",
            },
            terminal=terminal,
        )

    def create_upload(self, request: UploadCreateRequest) -> dict[str, Any]:
        dataset_id = uuid4()
        session = self.uploads.create(
            dataset_id,
            request.filename,
            request.size_bytes,
            request.source_format,
        )
        relative_source = f"datasets/{dataset_id}/source.{request.source_format}"
        manifest = DatasetManifest(
            dataset_id=dataset_id,
            source=ManifestFile(
                relative_path=relative_source,
                sha256=ZERO_SHA256,
                size_bytes=request.size_bytes,
            ),
            schema_version="0",
            rules_version="0",
            metadata={
                "filename": request.filename,
                "source_format": request.source_format,
            },
        )
        try:
            record = self.repository.create_dataset(manifest)
        except Exception:
            # The upload directory remains an unpublished staging session; no source
            # artifact has been made immutable at this point.
            raise
        self._save_control(
            dataset_id,
            {
                "version": "1.0",
                "filename": request.filename,
                "source_format": request.source_format,
                "declared_size_bytes": request.size_bytes,
            },
        )
        self._emit(dataset_id, stage="upload", state=DatasetState.UPLOADING, completed=0)
        return {
            "dataset_id": str(dataset_id),
            "upload_id": str(dataset_id),
            "state": record.state.value,
            "upload_offset": session.offset,
        }

    def upload_offset(self, dataset_id: UUID | str) -> int:
        record = self.repository.get_dataset(dataset_id)
        if record.state is not DatasetState.UPLOADING:
            raise DomainError(ErrorCode.INVALID_STATE, "dataset is not accepting upload content")
        return int(self.uploads.head_offset(dataset_id))

    def append_content(self, dataset_id: UUID | str, offset: int, body: bytes) -> int:
        record = self.repository.get_dataset(dataset_id)
        if record.state is not DatasetState.UPLOADING:
            raise DomainError(ErrorCode.INVALID_STATE, "dataset is not accepting upload content")
        return int(self.uploads.append(dataset_id, offset, body))

    def complete_upload(self, dataset_id: UUID | str, sha256: str) -> dict[str, Any]:
        record = self.repository.get_dataset(dataset_id)
        if record.state is not DatasetState.UPLOADING:
            raise DomainError(ErrorCode.INVALID_STATE, "only an uploading dataset can be completed")
        published = self.uploads.complete(dataset_id, sha256)
        manifest = self.repository.get_dataset_manifest(dataset_id)
        manifest = manifest.model_copy(
            update={
                "source": ManifestFile(
                    relative_path=published.relative_path,
                    sha256=published.sha256,
                    size_bytes=published.size_bytes,
                )
            }
        )
        self.repository.update_dataset_manifest(
            dataset_id,
            manifest,
            expected_state=DatasetState.UPLOADING,
        )
        self.repository.transition_dataset(dataset_id, DatasetState.STAGED)
        self._emit(dataset_id, stage="upload", state=DatasetState.STAGED)
        self.repository.transition_dataset(dataset_id, DatasetState.INSPECTING)
        self._emit(dataset_id, stage="inspect", state=DatasetState.INSPECTING, completed=0)
        try:
            inspected = self._inspect(dataset_id)
        except DomainError as exc:
            self._fail(dataset_id, exc)
            raise
        return {"dataset_id": str(dataset_id), "state": inspected.state.value}

    def _inspect(self, dataset_id: UUID | str) -> Any:
        control = self._control(dataset_id)
        manifest = self.repository.get_dataset_manifest(dataset_id)
        source_path = self.workspace.resolve_relative(
            manifest.source.relative_path, require_exists=True
        )
        source_format = control["source_format"]
        if source_format == "csv":
            from sts.ingest.csv import inspect_csv

            inspection = inspect_csv(source_path)
            control["parse_inspection"] = _jsonable(inspection)
            self._save_control(dataset_id, control)
            if bool(getattr(inspection, "requires_confirmation", False)):
                record = self.repository.transition_dataset(
                    dataset_id,
                    DatasetState.PARSE_OPTIONS_REQUIRED,
                    expected_state=DatasetState.INSPECTING,
                )
                self._emit(
                    dataset_id,
                    stage="inspect",
                    state=DatasetState.PARSE_OPTIONS_REQUIRED,
                    code="CSV_PARSE_OPTIONS_REQUIRED",
                )
                return record
            options = getattr(inspection, "options", None)
            proposal = getattr(inspection, "proposal", None)
            return self._convert_csv(dataset_id, options=options, proposal=proposal)
        if source_format == "xlsx":
            from sts.ingest.xlsx import preflight_xlsx

            inspection = preflight_xlsx(source_path)
            control["xlsx_inspection"] = _jsonable(inspection)
            self._save_control(dataset_id, control)
            if bool(getattr(inspection, "requires_sheet_selection", False)):
                record = self.repository.transition_dataset(
                    dataset_id,
                    DatasetState.SHEET_REQUIRED,
                    expected_state=DatasetState.INSPECTING,
                )
                self._emit(
                    dataset_id,
                    stage="inspect",
                    state=DatasetState.SHEET_REQUIRED,
                    code="XLSX_SHEET_REQUIRED",
                )
                return record
            selected = getattr(inspection, "automatically_selected_sheet", None)
            return self._convert_xlsx(dataset_id, selected_sheet=selected)
        raise DomainError(ErrorCode.INPUT_FORMAT_UNSUPPORTED, "source format is unsupported")

    def _raw_relative(self, dataset_id: UUID | str, attempt: int) -> str:
        return f"datasets/{dataset_id}/raw-attempt-{attempt}.parquet"

    def _publish_raw_metadata(
        self,
        dataset_id: UUID | str,
        *,
        raw_relative_path: str,
        row_count: int,
        column_count: int,
        skipped_records: int = 0,
    ) -> Any:
        if row_count <= 0:
            raise DomainError(ErrorCode.SCHEMA_INVALID, "input contains no records")
        if column_count <= 0 or column_count > 70:
            raise DomainError(
                ErrorCode.SCHEMA_INVALID,
                f"input column count must be between 1 and 70, got {column_count}",
            )
        raw_path = self.workspace.resolve_relative(raw_relative_path, require_exists=True)
        names = raw_columns(raw_path)
        if len(names) != column_count:
            raise DomainError(
                ErrorCode.SCHEMA_INVALID, "raw conversion column count is inconsistent"
            )
        control = self._control(dataset_id)
        control.update(
            {
                "raw_relative_path": raw_relative_path,
                "raw_row_count": row_count,
                "raw_column_count": column_count,
                "skipped_records": skipped_records,
            }
        )
        self._save_control(dataset_id, control)
        manifest = self.repository.get_dataset_manifest(dataset_id)
        metadata = dict(manifest.metadata)
        metadata.update(
            {
                "raw_relative_path": raw_relative_path,
                "raw_row_count": row_count,
                "raw_column_count": column_count,
                "malformed_skipped_records": skipped_records,
            }
        )
        self.repository.update_dataset_manifest(
            dataset_id,
            manifest.model_copy(update={"row_count": row_count, "metadata": metadata}),
            expected_state=DatasetState.INSPECTING,
        )
        record = self.repository.transition_dataset(
            dataset_id,
            DatasetState.RAW_READY,
            expected_state=DatasetState.INSPECTING,
        )
        self._emit(dataset_id, stage="inspect", state=DatasetState.RAW_READY)
        return record

    def _convert_csv(self, dataset_id: UUID | str, *, options: Any, proposal: Any = None) -> Any:
        from sts.ingest.csv import convert_csv_to_raw_parquet

        record = self.repository.get_dataset(dataset_id)
        manifest = self.repository.get_dataset_manifest(dataset_id)
        source_path = self.workspace.resolve_relative(
            manifest.source.relative_path, require_exists=True
        )
        relative = self._raw_relative(dataset_id, record.attempt)
        output = self.workspace.resolve_relative(relative)
        result = convert_csv_to_raw_parquet(
            source_path,
            output,
            options=options,
            proposal=proposal,
        )
        row_count = int(getattr(result, "rows", getattr(result, "row_count", 0)))
        column_count = int(getattr(result, "columns", getattr(result, "column_count", 0)))
        skipped = int(getattr(result, "skipped", getattr(result, "skipped_record_count", 0)))
        return self._publish_raw_metadata(
            dataset_id,
            raw_relative_path=relative,
            row_count=row_count,
            column_count=column_count,
            skipped_records=skipped,
        )

    def get_parse_options(self, dataset_id: UUID | str) -> dict[str, Any]:
        record = self.repository.get_dataset(dataset_id)
        control = self._control(dataset_id)
        inspection = control.get("parse_inspection")
        if inspection is None:
            raise DomainError(ErrorCode.INVALID_STATE, "dataset has no CSV parse proposal")
        assert isinstance(inspection, dict)
        return {
            "dataset_id": str(dataset_id),
            "state": record.state.value,
            "proposal": inspection.get("proposal"),
            "confirmation": control.get("confirmed_parse_options"),
            "malformed_preview": inspection.get("malformed_preview", []),
        }

    def put_parse_options(
        self, dataset_id: UUID | str, request: ParseOptionsRequest
    ) -> dict[str, Any]:
        record = self.repository.get_dataset(dataset_id)
        if record.state is not DatasetState.PARSE_OPTIONS_REQUIRED:
            raise DomainError(
                ErrorCode.INVALID_STATE, "dataset is not waiting for CSV parse options"
            )
        from sts.ingest.csv import CsvParseOptions

        try:
            options = CsvParseOptions(
                encoding=request.encoding,
                delimiter=request.delimiter,
                malformed=request.malformed,
                confirmed=True,
            )
        except (TypeError, ValueError, ValidationError) as exc:
            raise DomainError(
                ErrorCode.SCHEMA_INVALID, f"invalid CSV parse options: {exc}"
            ) from exc
        self.repository.transition_dataset(
            dataset_id,
            DatasetState.INSPECTING,
            expected_state=DatasetState.PARSE_OPTIONS_REQUIRED,
        )
        control = self._control(dataset_id)
        control["confirmed_parse_options"] = request.model_dump(mode="json")
        self._save_control(dataset_id, control)
        try:
            result = self._convert_csv(dataset_id, options=options)
        except DomainError as exc:
            self._fail(dataset_id, exc)
            raise
        return {"dataset_id": str(dataset_id), "state": result.state.value}

    def _convert_xlsx(self, dataset_id: UUID | str, *, selected_sheet: str | None) -> Any:
        from sts.ingest.xlsx import convert_xlsx_to_raw_parquet

        if not selected_sheet:
            raise DomainError(ErrorCode.INVALID_STATE, "an XLSX sheet must be selected")
        record = self.repository.get_dataset(dataset_id)
        manifest = self.repository.get_dataset_manifest(dataset_id)
        source_path = self.workspace.resolve_relative(
            manifest.source.relative_path, require_exists=True
        )
        relative = self._raw_relative(dataset_id, record.attempt)
        output = self.workspace.resolve_relative(relative)
        result = convert_xlsx_to_raw_parquet(
            source_path,
            output,
            selected_sheet=selected_sheet,
        )
        row_count = int(getattr(result, "rows", getattr(result, "row_count", 0)))
        column_count = int(getattr(result, "columns", getattr(result, "column_count", 0)))
        return self._publish_raw_metadata(
            dataset_id,
            raw_relative_path=relative,
            row_count=row_count,
            column_count=column_count,
        )

    def get_sheets(self, dataset_id: UUID | str) -> dict[str, Any]:
        record = self.repository.get_dataset(dataset_id)
        inspection = self._control(dataset_id).get("xlsx_inspection")
        if inspection is None:
            raise DomainError(ErrorCode.INVALID_STATE, "dataset has no XLSX sheet inspection")
        assert isinstance(inspection, dict)
        return {
            "dataset_id": str(dataset_id),
            "state": record.state.value,
            "sheets": inspection.get("sheets", []),
            "requires_sheet_selection": inspection.get("requires_sheet_selection", True),
            "selected_sheet": self._control(dataset_id).get("selected_sheet"),
        }

    def put_sheet(self, dataset_id: UUID | str, request: SheetSelectionRequest) -> dict[str, Any]:
        record = self.repository.get_dataset(dataset_id)
        if record.state is not DatasetState.SHEET_REQUIRED:
            raise DomainError(ErrorCode.INVALID_STATE, "dataset is not waiting for sheet selection")
        from sts.ingest.xlsx import preflight_xlsx, select_xlsx_sheet

        manifest = self.repository.get_dataset_manifest(dataset_id)
        source_path = self.workspace.resolve_relative(
            manifest.source.relative_path, require_exists=True
        )
        inspection = preflight_xlsx(source_path)
        selected = select_xlsx_sheet(inspection, request.name)
        selected_name = selected.name
        control = self._control(dataset_id)
        control["selected_sheet"] = selected_name
        self._save_control(dataset_id, control)
        self.repository.transition_dataset(
            dataset_id,
            DatasetState.INSPECTING,
            expected_state=DatasetState.SHEET_REQUIRED,
        )
        try:
            result = self._convert_xlsx(dataset_id, selected_sheet=selected_name)
        except DomainError as exc:
            self._fail(dataset_id, exc)
            raise
        return {"dataset_id": str(dataset_id), "state": result.state.value}

    def start_profile(self, dataset_id: UUID | str) -> dict[str, Any]:
        self.repository.transition_dataset(
            dataset_id,
            DatasetState.PROFILING,
            expected_state=DatasetState.RAW_READY,
        )
        self._emit(dataset_id, stage="profile", state=DatasetState.PROFILING, completed=0)
        try:
            record = self._run_profile(dataset_id)
        except DomainError as exc:
            self._fail(dataset_id, exc)
            raise
        return {"dataset_id": str(dataset_id), "state": record.state.value}

    def _run_profile(self, dataset_id: UUID | str) -> Any:
        record = self.repository.get_dataset(dataset_id)
        control = self._control(dataset_id)
        raw_relative = str(control["raw_relative_path"])
        raw_path = self.workspace.resolve_relative(raw_relative, require_exists=True)
        profile = profile_parquet(
            raw_path,
            view="raw",
            memory_limit=self.duckdb_memory_limit,
            temp_directory=self._directory(dataset_id) / ".profile-spill",
        )
        relative = f"datasets/{dataset_id}/profile-raw-attempt-{record.attempt}.json"
        _atomic_json(self.workspace.resolve_relative(relative), profile)
        control["raw_profile_relative_path"] = relative
        self._save_control(dataset_id, control)
        result = self.repository.transition_dataset(
            dataset_id,
            DatasetState.PROFILED,
            expected_state=DatasetState.PROFILING,
        )
        self._emit(dataset_id, stage="profile", state=DatasetState.PROFILED)
        return result

    def get_profile(self, dataset_id: UUID | str, view: Literal["raw", "typed"]) -> DatasetProfile:
        self.repository.get_dataset(dataset_id)
        key = "raw_profile_relative_path" if view == "raw" else "typed_profile_relative_path"
        relative = self._control(dataset_id).get(key)
        if not relative:
            raise DomainError(ErrorCode.INVALID_STATE, f"{view} profile is not available")
        path = self.workspace.resolve_relative(str(relative), require_exists=True)
        return DatasetProfile.model_validate_json(path.read_bytes())

    def put_schema(self, dataset_id: UUID | str, request: SchemaRequest) -> dict[str, Any]:
        record = self.repository.get_dataset(dataset_id)
        if record.state is not DatasetState.PROFILED:
            raise DomainError(
                ErrorCode.INVALID_STATE, "schema can only be saved for a profiled dataset"
            )
        control = self._control(dataset_id)
        raw_path = self.workspace.resolve_relative(
            str(control["raw_relative_path"]), require_exists=True
        )
        validate_schema(raw_path, request.columns)
        digest = hashlib.sha256(
            json.dumps(
                [column.model_dump(mode="json") for column in request.columns],
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        schema_version = digest[:16]
        relative = f"datasets/{dataset_id}/schema-{digest}.json"
        schema_path = self.workspace.resolve_relative(relative)
        if not schema_path.exists():
            _atomic_json(schema_path, {"version": "1.0", "columns": request.columns})
        control.update(
            {
                "schema_relative_path": relative,
                "schema_version": schema_version,
            }
        )
        self._save_control(dataset_id, control)
        manifest = self.repository.get_dataset_manifest(dataset_id)
        self.repository.update_dataset_manifest(
            dataset_id,
            manifest.model_copy(
                update={
                    "schema_version": schema_version,
                    "columns": request.columns,
                }
            ),
            expected_state=DatasetState.PROFILED,
        )
        result = self.repository.transition_dataset(
            dataset_id,
            DatasetState.SCHEMA_READY,
            expected_state=DatasetState.PROFILED,
        )
        self._emit(dataset_id, stage="schema", state=DatasetState.SCHEMA_READY)
        return {
            "dataset_id": str(dataset_id),
            "state": result.state.value,
            "schema_version": schema_version,
        }

    def put_rules(self, dataset_id: UUID | str, request: RulesRequest) -> dict[str, Any]:
        record = self.repository.get_dataset(dataset_id)
        if record.state is not DatasetState.SCHEMA_READY:
            raise DomainError(
                ErrorCode.INVALID_STATE,
                "rules can only be saved after schema validation and before normalization",
            )
        manifest = self.repository.get_dataset_manifest(dataset_id)
        compiled = compile_rules(
            manifest.columns,
            tuple(rule.value for rule in request.rules),
            mode="utility",
        )
        payload = {
            "version": "1.0",
            "rules": [rule.model_dump(mode="json") for rule in compiled.rules],
        }
        digest = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
        rules_version = digest[:16]
        relative = f"datasets/{dataset_id}/rules-{digest}.json"
        rules_path = self.workspace.resolve_relative(relative)
        if not rules_path.exists():
            _atomic_json(rules_path, payload)
        control = self._control(dataset_id)
        control.update(
            {
                "rules_relative_path": relative,
                "rules_version": rules_version,
                "rules_sha256": digest,
            }
        )
        self._save_control(dataset_id, control)
        metadata = dict(manifest.metadata)
        metadata["rules_relative_path"] = relative
        updated = manifest.model_copy(
            update={
                "rules_version": rules_version,
                "rules_sha256": digest,
                "metadata": metadata,
            }
        )
        result = self.repository.update_dataset_manifest(
            dataset_id,
            updated,
            expected_state=DatasetState.SCHEMA_READY,
        )
        self._emit(dataset_id, stage="rules", state=DatasetState.SCHEMA_READY)
        return {
            "dataset_id": str(dataset_id),
            "state": result.state.value,
            "rules_version": rules_version,
            "rules_sha256": digest,
            "rule_count": len(compiled.rules),
        }

    def start_normalize(self, dataset_id: UUID | str) -> dict[str, Any]:
        self.repository.transition_dataset(
            dataset_id,
            DatasetState.NORMALIZING,
            expected_state=DatasetState.SCHEMA_READY,
        )
        self._emit(dataset_id, stage="normalize", state=DatasetState.NORMALIZING, completed=0)
        try:
            record = self._run_normalize(dataset_id)
        except DomainError as exc:
            self._fail(dataset_id, exc)
            raise
        return {"dataset_id": str(dataset_id), "state": record.state.value}

    def _run_normalize(self, dataset_id: UUID | str) -> Any:
        record = self.repository.get_dataset(dataset_id)
        control = self._control(dataset_id)
        raw_path = self.workspace.resolve_relative(
            str(control["raw_relative_path"]), require_exists=True
        )
        manifest = self.repository.get_dataset_manifest(dataset_id)
        relative = (
            f"datasets/{dataset_id}/normalized-{manifest.schema_version}"
            f"-attempt-{record.attempt}.parquet"
        )
        output = self.workspace.resolve_relative(relative)
        result = normalize_to_parquet(
            raw_path,
            output,
            manifest.columns,
            memory_limit=self.duckdb_memory_limit,
            temp_directory=self._directory(dataset_id) / ".normalize-spill",
        )
        typed = profile_parquet(
            result.path,
            view="typed",
            memory_limit=self.duckdb_memory_limit,
            temp_directory=self._directory(dataset_id) / ".profile-spill",
        )
        typed_relative = f"datasets/{dataset_id}/profile-typed-attempt-{record.attempt}.json"
        _atomic_json(self.workspace.resolve_relative(typed_relative), typed)
        control.update(
            {
                "normalized_relative_path": relative,
                "typed_profile_relative_path": typed_relative,
            }
        )
        self._save_control(dataset_id, control)
        normalized = ManifestFile(
            relative_path=relative,
            sha256=result.sha256,
            size_bytes=result.size_bytes,
        )
        self.repository.update_dataset_manifest(
            dataset_id,
            manifest.model_copy(update={"normalized": normalized, "row_count": result.row_count}),
            expected_state=DatasetState.NORMALIZING,
        )
        completed = self.repository.transition_dataset(
            dataset_id,
            DatasetState.NORMALIZED,
            expected_state=DatasetState.NORMALIZING,
        )
        self._emit(
            dataset_id,
            stage="normalize",
            state=DatasetState.NORMALIZED,
            terminal=True,
            code="DATASET_NORMALIZED",
        )
        return completed

    def retry(self, dataset_id: UUID | str) -> RetryResponse:
        record = self.repository.retry_dataset(dataset_id)
        self._emit(
            dataset_id,
            stage="retry",
            state=record.state,
            completed=0,
            code="DATASET_RETRY_STARTED",
        )
        return RetryResponse(
            dataset_id=UUID(str(dataset_id)), state=record.state, attempt=record.attempt
        )

    def _fail(self, dataset_id: UUID | str, error: DomainError) -> None:
        record = self.repository.get_dataset(dataset_id)
        if record.state not in {
            DatasetState.INSPECTING,
            DatasetState.PROFILING,
            DatasetState.NORMALIZING,
        }:
            return
        failed = self.repository.transition_dataset(
            dataset_id,
            DatasetState.FAILED,
            expected_state=record.state,
            error_code=error.code,
        )
        self._emit(
            dataset_id,
            stage=record.state.value,
            state=failed.state,
            terminal=True,
            code=error.code.value,
        )

    def get_dataset(self, dataset_id: UUID | str) -> dict[str, Any]:
        record = self.repository.get_dataset(dataset_id)
        actions = {
            DatasetState.UPLOADING: ["upload_content", "complete"],
            DatasetState.PARSE_OPTIONS_REQUIRED: ["confirm_parse_options"],
            DatasetState.SHEET_REQUIRED: ["select_sheet"],
            DatasetState.RAW_READY: ["profile"],
            DatasetState.PROFILED: ["save_schema"],
            DatasetState.SCHEMA_READY: ["normalize"],
            DatasetState.FAILED: ["retry"],
        }.get(record.state, [])
        events = self.repository.replay_events(OwnerType.DATASET, dataset_id)
        latest = events[-1] if events else None
        return {
            "dataset_id": str(record.dataset_id),
            "state": record.state.value,
            "attempt": record.attempt,
            "manifest_sha256": record.manifest_sha256,
            "progress": latest.payload if latest else None,
            "legal_actions": actions,
        }


def create_dataset_router(service: DatasetService) -> APIRouter:
    router = APIRouter(prefix="/api/v1/datasets", tags=["datasets"])

    @router.post("/uploads", status_code=status.HTTP_201_CREATED)
    def create_upload(body: UploadCreateRequest) -> dict[str, Any]:
        return service.create_upload(body)

    @router.head("/{dataset_id}/content", status_code=status.HTTP_204_NO_CONTENT)
    def head_content(dataset_id: UUID) -> Response:
        return Response(
            status_code=204,
            headers={"Upload-Offset": str(service.upload_offset(dataset_id))},
        )

    @router.patch("/{dataset_id}/content", status_code=status.HTTP_204_NO_CONTENT)
    async def patch_content(
        dataset_id: UUID,
        request: Request,
        upload_offset: int = Header(alias="Upload-Offset", ge=0),
    ) -> Response:
        body = await request.body()
        new_offset = service.append_content(dataset_id, upload_offset, body)
        return Response(status_code=204, headers={"Upload-Offset": str(new_offset)})

    @router.post("/{dataset_id}/complete", status_code=status.HTTP_202_ACCEPTED)
    def complete_upload(dataset_id: UUID, body: UploadCompleteRequest) -> dict[str, Any]:
        return service.complete_upload(dataset_id, body.sha256)

    @router.get("/{dataset_id}")
    def get_dataset(dataset_id: UUID) -> dict[str, Any]:
        return service.get_dataset(dataset_id)

    @router.get("/{dataset_id}/parse-options")
    def get_parse_options(dataset_id: UUID) -> dict[str, Any]:
        return service.get_parse_options(dataset_id)

    @router.put("/{dataset_id}/parse-options")
    def put_parse_options(dataset_id: UUID, body: ParseOptionsRequest) -> dict[str, Any]:
        return service.put_parse_options(dataset_id, body)

    @router.get("/{dataset_id}/sheets")
    def get_sheets(dataset_id: UUID) -> dict[str, Any]:
        return service.get_sheets(dataset_id)

    @router.put("/{dataset_id}/sheet")
    def put_sheet(dataset_id: UUID, body: SheetSelectionRequest) -> dict[str, Any]:
        return service.put_sheet(dataset_id, body)

    @router.post("/{dataset_id}/profile", status_code=status.HTTP_202_ACCEPTED)
    def profile(dataset_id: UUID) -> dict[str, Any]:
        return service.start_profile(dataset_id)

    @router.get("/{dataset_id}/profile")
    def get_profile(
        dataset_id: UUID,
        view: Literal["raw", "typed"] = "raw",
    ) -> dict[str, Any]:
        return service.get_profile(dataset_id, view).model_dump(mode="json")

    @router.put("/{dataset_id}/schema")
    def put_schema(dataset_id: UUID, body: SchemaRequest) -> dict[str, Any]:
        return service.put_schema(dataset_id, body)

    @router.put("/{dataset_id}/rules")
    def put_rules(dataset_id: UUID, body: RulesRequest) -> dict[str, Any]:
        return service.put_rules(dataset_id, body)

    @router.post("/{dataset_id}/normalize", status_code=status.HTTP_202_ACCEPTED)
    def normalize(dataset_id: UUID) -> dict[str, Any]:
        return service.start_normalize(dataset_id)

    @router.get("/{dataset_id}/events")
    def events(dataset_id: UUID, request: Request) -> Response:
        service.repository.get_dataset(dataset_id)
        return dataset_event_response(service.repository, dataset_id, request)

    @router.post("/{dataset_id}/retry", status_code=status.HTTP_202_ACCEPTED)
    def retry(dataset_id: UUID) -> dict[str, Any]:
        return service.retry(dataset_id).model_dump(mode="json")

    return router
