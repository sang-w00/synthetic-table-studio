from __future__ import annotations

from enum import StrEnum

from .errors import DomainError, ErrorCode


class DatasetState(StrEnum):
    UPLOADING = "uploading"
    STAGED = "staged"
    INSPECTING = "inspecting"
    PARSE_OPTIONS_REQUIRED = "parse_options_required"
    SHEET_REQUIRED = "sheet_required"
    RAW_READY = "raw_ready"
    PROFILING = "profiling"
    PROFILED = "profiled"
    SCHEMA_READY = "schema_ready"
    NORMALIZING = "normalizing"
    NORMALIZED = "normalized"
    FAILED = "failed"


DATASET_TRANSITIONS: dict[DatasetState, frozenset[DatasetState]] = {
    DatasetState.UPLOADING: frozenset({DatasetState.STAGED}),
    DatasetState.STAGED: frozenset({DatasetState.INSPECTING}),
    DatasetState.INSPECTING: frozenset(
        {
            DatasetState.PARSE_OPTIONS_REQUIRED,
            DatasetState.SHEET_REQUIRED,
            DatasetState.RAW_READY,
            DatasetState.FAILED,
        }
    ),
    DatasetState.PARSE_OPTIONS_REQUIRED: frozenset({DatasetState.INSPECTING}),
    DatasetState.SHEET_REQUIRED: frozenset({DatasetState.INSPECTING}),
    DatasetState.RAW_READY: frozenset({DatasetState.PROFILING}),
    DatasetState.PROFILING: frozenset({DatasetState.PROFILED, DatasetState.FAILED}),
    DatasetState.PROFILED: frozenset({DatasetState.SCHEMA_READY}),
    DatasetState.SCHEMA_READY: frozenset({DatasetState.NORMALIZING}),
    DatasetState.NORMALIZING: frozenset({DatasetState.NORMALIZED, DatasetState.FAILED}),
    DatasetState.NORMALIZED: frozenset(),
    DatasetState.FAILED: frozenset(),
}

DATASET_RETRY_STATES = frozenset(
    {DatasetState.INSPECTING, DatasetState.PROFILING, DatasetState.NORMALIZING}
)


class JobState(StrEnum):
    QUEUED = "queued"
    ADMITTED = "admitted"
    PREPARING = "preparing"
    FITTING = "fitting"
    GENERATING = "generating"
    REPAIRING = "repairing"
    EVALUATING = "evaluating"
    EXPORTING = "exporting"
    PUBLISHING = "publishing"
    SUCCEEDED = "succeeded"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"
    FAILED = "failed"


JOB_TERMINAL_STATES = frozenset({JobState.SUCCEEDED, JobState.CANCELLED, JobState.FAILED})
JOB_RUNNING_STATES = frozenset(
    {
        JobState.QUEUED,
        JobState.ADMITTED,
        JobState.PREPARING,
        JobState.FITTING,
        JobState.GENERATING,
        JobState.REPAIRING,
        JobState.EVALUATING,
        JobState.EXPORTING,
        JobState.PUBLISHING,
    }
)

_JOB_FORWARD: dict[JobState, JobState] = {
    JobState.QUEUED: JobState.ADMITTED,
    JobState.ADMITTED: JobState.PREPARING,
    JobState.PREPARING: JobState.FITTING,
    JobState.FITTING: JobState.GENERATING,
    JobState.GENERATING: JobState.REPAIRING,
    JobState.REPAIRING: JobState.EVALUATING,
    JobState.EVALUATING: JobState.EXPORTING,
    JobState.EXPORTING: JobState.PUBLISHING,
    JobState.PUBLISHING: JobState.SUCCEEDED,
}

JOB_TRANSITIONS: dict[JobState, frozenset[JobState]] = {}
for _state in JobState:
    _next: set[JobState] = set()
    if _state in _JOB_FORWARD:
        _next.add(_JOB_FORWARD[_state])
    if _state in JOB_RUNNING_STATES:
        _next.add(JobState.CANCELLING)
    if _state not in JOB_TERMINAL_STATES:
        _next.add(JobState.FAILED)
    if _state is JobState.CANCELLING:
        _next.add(JobState.CANCELLED)
    JOB_TRANSITIONS[_state] = frozenset(_next)


def validate_dataset_transition(
    current: DatasetState | str, target: DatasetState | str
) -> DatasetState:
    current_state = DatasetState(current)
    target_state = DatasetState(target)
    if target_state not in DATASET_TRANSITIONS[current_state]:
        raise DomainError(
            ErrorCode.INVALID_STATE,
            f"dataset cannot transition from {current_state.value} to {target_state.value}",
            context={"current": current_state.value, "target": target_state.value},
        )
    return target_state


def validate_job_transition(current: JobState | str, target: JobState | str) -> JobState:
    current_state = JobState(current)
    target_state = JobState(target)
    if target_state not in JOB_TRANSITIONS[current_state]:
        raise DomainError(
            ErrorCode.INVALID_STATE,
            f"job cannot transition from {current_state.value} to {target_state.value}",
            context={"current": current_state.value, "target": target_state.value},
        )
    return target_state


def is_job_terminal(state: JobState | str) -> bool:
    return JobState(state) in JOB_TERMINAL_STATES
