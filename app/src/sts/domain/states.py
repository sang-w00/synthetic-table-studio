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
    # PROFILED is the reopen path: a normalize that fails because the declared column
    # types do not fit the data is a correctable input error, not a dead end, so the
    # dataset returns to the schema step instead of terminating in FAILED.
    DatasetState.NORMALIZING: frozenset(
        {DatasetState.NORMALIZED, DatasetState.FAILED, DatasetState.PROFILED}
    ),
    # Reopen edges: a normalized (or schema-ready) dataset can be returned to the
    # schema step so its schema and rules can be edited and normalization re-run.
    DatasetState.NORMALIZED: frozenset({DatasetState.PROFILED}),
    # Retry returns a failed dataset to the stable state that precedes the failed
    # operation, from which the same operation is dispatched again.
    DatasetState.FAILED: frozenset(
        {DatasetState.STAGED, DatasetState.RAW_READY, DatasetState.SCHEMA_READY}
    ),
}
DATASET_TRANSITIONS[DatasetState.SCHEMA_READY] = frozenset(
    {DatasetState.NORMALIZING, DatasetState.PROFILED}
)

DATASET_RETRY_STATES = frozenset(
    {DatasetState.INSPECTING, DatasetState.PROFILING, DatasetState.NORMALIZING}
)

# Failed operation -> the stable state the dataset is returned to on retry.
DATASET_RETRY_RESTART_STATES: dict[DatasetState, DatasetState] = {
    DatasetState.INSPECTING: DatasetState.STAGED,
    DatasetState.PROFILING: DatasetState.RAW_READY,
    DatasetState.NORMALIZING: DatasetState.SCHEMA_READY,
}

DATASET_REOPEN_STATES = frozenset({DatasetState.NORMALIZED, DatasetState.SCHEMA_READY})


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
