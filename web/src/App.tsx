import { lazy, Suspense, useEffect, useMemo, useRef, useState } from "react";
import type { KeyboardEvent } from "react";

import {
  ApiProblem,
  api,
  type ArtifactManifest,
  type ColumnKind,
  type ColumnRole,
  type ColumnSchema,
  type ExecutiveSummary,
  type DatasetProfile,
  type JobSnapshot,
  type HostResources,
  type DifferentialPrivacySynthesisRequest,
  type ManifestFile,
  type ParseOptions,
  type PrimaryReport,
  type ProgressEventPayload,
  type RecoverableDataset,
  type RuleKind,
  type RuleSpec,
  type ResourcePlan,
  type SheetDescriptor,
  type UploadProgress,
  type SynthesisRequest,
  type UtilitySynthesisRequest,
} from "./api";
import { findRuleConflicts } from "./rules";
// The chart is the only ECharts consumer; lazy loading keeps it out of the six-step workflow shell.
const ReportChart = lazy(async () => {
  const module = await import("./ReportChart");
  return { default: module.ReportChart };
});

type Stage = "upload" | "schema" | "rules" | "mode" | "progress" | "report";
type UploadBranch = "none" | "parse" | "sheet";
type ReportTab = "summary" | "columns" | "boundary";

const STAGES: Array<{ id: Stage; label: string; hint: string }> = [
  { id: "upload", label: "업로드", hint: "파일 검사" },
  { id: "schema", label: "스키마", hint: "열 의미 확인" },
  { id: "rules", label: "무결성 규칙", hint: "제약 정의" },
  { id: "mode", label: "생성 모드", hint: "자원 설정" },
  { id: "progress", label: "진행", hint: "작업 관찰" },
  { id: "report", label: "보고서", hint: "품질·다운로드" },
];

const KIND_LABELS: Record<ColumnKind, string> = {
  integer: "정수",
  fixed_decimal: "고정 소수",
  float: "실수",
  categorical: "범주",
  boolean: "참/거짓",
  date: "날짜",
  datetime: "날짜·시간",
  text: "텍스트",
  identifier: "식별자",
  excluded: "제외",
};

const ROLE_LABELS: Record<ColumnRole, string> = {
  model: "모델링",
  derived: "파생",
  identifier: "새 식별자",
  excluded: "출력 제외",
};

const RULE_LABELS: Record<RuleKind, string> = {
  mask_prefix: "접두부 마스킹",
  not_null: "필수값",
  allowed_values: "허용값",
  range: "범위",
  fixed_combination: "고정 조합",
  conditional_set: "조건부 고정값",
  sum_equals: "합계 일치",
  compare: "열 비교",
};

const JOB_STAGE_LABELS: Record<string, string> = {
  queued: "대기열",
  admission: "자원 승인",
  admitted: "승인됨",
  preparing: "데이터 준비",
  fitting: "모델 학습",
  generating: "행 생성",
  repairing: "규칙 재구성",
  evaluating: "품질 평가",
  exporting: "파일 내보내기",
  publishing: "결과 게시",
  cancelling: "취소 처리",
  cancelled: "취소됨",
  succeeded: "완료",
  failed: "실패",
  resume: "재개",
};

interface RuleDraft {
  kind: RuleKind;
  column: string;
  secondary: string;
  tertiary: string;
  value: string;
  values: string;
  min: string;
  max: string;
  keepChars: string;
  tolerance: string;
  operator: string;
  tuples: string;
  provenance: "public" | "private_inferred";
  sourceAction: "block" | "drop_row";
}

const INITIAL_RULE_DRAFT: RuleDraft = {
  kind: "not_null",
  column: "",
  secondary: "",
  tertiary: "",
  value: "",
  values: "",
  min: "",
  max: "",
  keepChars: "1",
  tolerance: "0",
  operator: "=",
  tuples: "",
  provenance: "public",
  sourceAction: "block",
};

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MiB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GiB`;
}

function displayError(error: unknown): string {
  if (error instanceof ApiProblem) return `${error.code}: ${error.message}`;
  if (error instanceof Error) return error.message;
  return "알 수 없는 오류가 발생했습니다.";
}

function formatGiB(bytes: number): string {
  return `${(bytes / 1024 ** 3).toLocaleString("ko-KR", { maximumFractionDigits: 1 })} GiB`;
}

function schemaFromProfile(profile: DatasetProfile): ColumnSchema[] {
  return profile.columns.map((column) => {
    const role: ColumnRole =
      column.candidate_type === "identifier"
        ? "identifier"
        : column.candidate_type === "excluded"
          ? "excluded"
          : "model";
    return {
      name: column.name,
      kind: column.candidate_type,
      nullable: column.null_count > 0,
      role,
      ...(column.candidate_type === "fixed_decimal" ? { decimal_places: 2 } : {}),
      ...(column.candidate_type === "identifier" ? { identifier_strategy: "sequential" as const } : {}),
    };
  });
}

function makeRule(draft: RuleDraft): RuleSpec {
  const id = `r_${crypto.randomUUID()}`;
  const base = {
    id,
    provenance: draft.provenance,
    source_action: draft.sourceAction,
  } as const;
  const commaValues = draft.values
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  switch (draft.kind) {
    case "mask_prefix":
      return { ...base, kind: draft.kind, column: draft.column, keep_chars: Number(draft.keepChars) };
    case "not_null":
      return { ...base, kind: draft.kind, column: draft.column };
    case "allowed_values":
      return { ...base, kind: draft.kind, column: draft.column, values: commaValues };
    case "range":
      return {
        ...base,
        kind: draft.kind,
        column: draft.column,
        min: draft.min,
        max: draft.max,
        inclusive_min: true,
        inclusive_max: true,
      };
    case "fixed_combination": {
      const tuples = draft.tuples
        .split(";")
        .map((tuple) => tuple.split(",").map((value) => value.trim()))
        .filter((tuple) => tuple.some(Boolean));
      return {
        ...base,
        kind: draft.kind,
        columns: commaValues,
        ...(tuples.length > 0 ? { allowed_tuples: tuples } : {}),
      };
    }
    case "conditional_set":
      return {
        ...base,
        kind: draft.kind,
        when: { column: draft.column, operator: draft.operator, value: draft.value },
        target: draft.secondary,
        value: draft.tertiary,
      };
    case "sum_equals":
      return {
        ...base,
        kind: draft.kind,
        sources: commaValues,
        target: draft.secondary,
        tolerance: draft.tolerance,
      };
    case "compare":
      return {
        ...base,
        kind: draft.kind,
        left: draft.column,
        op: draft.operator as "<" | "<=" | ">" | ">=",
        right: draft.secondary,
        ...(draft.value ? { granularity: draft.value } : {}),
      };
  }
}

function validateRuleDraft(draft: RuleDraft): string | null {
  if (["mask_prefix", "not_null", "allowed_values", "range", "conditional_set", "compare"].includes(draft.kind) && !draft.column) {
    return "대상 열을 선택하세요.";
  }
  if (draft.kind === "mask_prefix" && (!Number.isInteger(Number(draft.keepChars)) || Number(draft.keepChars) < 0)) {
    return "유지할 문자 수는 0 이상의 정수여야 합니다.";
  }
  if (draft.kind === "allowed_values" && !draft.values.trim()) return "허용값을 하나 이상 입력하세요.";
  if (draft.kind === "range" && (!draft.min || !draft.max)) return "최솟값과 최댓값을 입력하세요.";
  if (draft.kind === "fixed_combination" && draft.values.split(",").filter(Boolean).length < 2) {
    return "고정 조합에는 두 개 이상의 열을 입력하세요.";
  }
  if (draft.kind === "conditional_set" && (!draft.secondary || !draft.tertiary)) {
    return "조건의 대상 열과 고정값을 입력하세요.";
  }
  if (draft.kind === "sum_equals" && (!draft.values || !draft.secondary)) {
    return "합산 원본 열과 대상 열을 입력하세요.";
  }
  if (draft.kind === "compare" && !draft.secondary) return "오른쪽 열을 선택하세요.";
  return null;
}

export function App() {
  const [stage, setStage] = useState<Stage>("upload");
  const [highestStage, setHighestStage] = useState(0);
  const [sessionReady, setSessionReady] = useState(false);
  const [globalStatus, setGlobalStatus] = useState("준비 중입니다.");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [hostResources, setHostResources] = useState<HostResources | null>(null);
  const [resourcePlan, setResourcePlan] = useState<ResourcePlan | null>(null);

  const [file, setFile] = useState<File | null>(null);
  const [uploadProgress, setUploadProgress] = useState<UploadProgress | null>(null);
  const [datasetId, setDatasetId] = useState<string | null>(null);
  const [datasetManifestSha, setDatasetManifestSha] = useState<string | null>(null);
  const [uploadBranch, setUploadBranch] = useState<UploadBranch>("none");
  const [recoverableUpload, setRecoverableUpload] = useState<RecoverableDataset | null>(null);
  const [parseOptions, setParseOptions] = useState<ParseOptions>({
    encoding: "utf-8",
    delimiter: ",",
    quotechar: '"',
    escapechar: null,
    has_header: true,
    malformed: "fail",
  });
  const [malformedPreview, setMalformedPreview] = useState<Array<Record<string, unknown> | string>>([]);
  const [sheets, setSheets] = useState<SheetDescriptor[]>([]);
  const [selectedSheet, setSelectedSheet] = useState("");

  const [profile, setProfile] = useState<DatasetProfile | null>(null);
  const [columns, setColumns] = useState<ColumnSchema[]>([]);
  const [schemaVersion, setSchemaVersion] = useState("0");
  const [schemaQuery, setSchemaQuery] = useState("");
  const [schemaNeedsReviewOnly, setSchemaNeedsReviewOnly] = useState(false);

  const [rules, setRules] = useState<RuleSpec[]>([]);
  const [ruleDraft, setRuleDraft] = useState<RuleDraft>(INITIAL_RULE_DRAFT);
  const [ruleDraftError, setRuleDraftError] = useState<string | null>(null);
  const [rulesVersion, setRulesVersion] = useState("0");

  const [synthesisMode, setSynthesisMode] = useState<"utility" | "differential_privacy">("utility");
  const [publicMetadata, setPublicMetadata] = useState<ManifestFile | null>(null);
  const [publicMetadataName, setPublicMetadataName] = useState("");
  const [epsilonModel, setEpsilonModel] = useState("3");
  const [delta, setDelta] = useState("0.000001");
  const [fitSamplingRate, setFitSamplingRate] = useState("1");
  const [samplingSeed, setSamplingSeed] = useState(927);
  const [outputRows, setOutputRows] = useState(100_000);
  const [outputFormats, setOutputFormats] = useState<Array<"parquet" | "csv">>(["parquet"]);
  const [trainingRows, setTrainingRows] = useState(50_000);
  const [maxEpochs, setMaxEpochs] = useState(5);
  const [maxMinutes, setMaxMinutes] = useState(60);
  const [modelSize, setModelSize] = useState("small");
  const [device, setDevice] = useState("cpu");
  const [resourceProfile, setResourceProfile] = useState("auto_cpu");
  const [generationSeed, setGenerationSeed] = useState(20260723);

  const [job, setJob] = useState<JobSnapshot | null>(null);
  const [jobProgress, setJobProgress] = useState<ProgressEventPayload | null>(null);
  const [report, setReport] = useState<PrimaryReport | null>(null);
  const [artifacts, setArtifacts] = useState<ArtifactManifest[]>([]);
  const [reportTab, setReportTab] = useState<ReportTab>("summary");
  const eventSource = useRef<EventSource | null>(null);
  const stageFocusRequested = useRef(false);

  const conflicts = useMemo(() => findRuleConflicts(rules), [rules]);
  const visibleColumns = useMemo(
    () => columns
      .map((column, index) => ({ column, index, profile: profile?.columns[index] }))
      .filter(({ column, profile: observed }) => (
        column.name.toLocaleLowerCase().includes(schemaQuery.trim().toLocaleLowerCase())
        && (!schemaNeedsReviewOnly || observed?.candidate_requires_confirmation === true)
      )),
    [columns, profile, schemaNeedsReviewOnly, schemaQuery],
  );
  const currentStageIndex = STAGES.findIndex((item) => item.id === stage);

  useEffect(() => {
    document.documentElement.lang = "ko";
    void api
      .bootstrap()
      .then(async (bootstrap) => {
        setHostResources(bootstrap.host_resources);
        setResourcePlan(bootstrap.resource_plan);
        setResourceProfile(bootstrap.resource_plan.resource_profile);
        setDevice(bootstrap.resource_plan.recommended_device);
        setTrainingRows((current) => Math.min(current, bootstrap.resource_plan.utility_max_rows));
        setSessionReady(true);
        setGlobalStatus("현재 시스템 자원을 확인하고 실행 설정을 준비했습니다.");
        try {
          await recoverWorkspace();
        } catch (recoveryError) {
          setGlobalStatus(
            `이전 작업을 복원하지 못했습니다. 새 업로드는 계속할 수 있습니다: ${displayError(recoveryError)}`,
          );
        }
      })
      .catch((bootstrapError: unknown) => {
        setError(`SESSION_REQUIRED: ${displayError(bootstrapError)}`);
        setGlobalStatus("초기화에 실패했습니다.");
      });
    return () => eventSource.current?.close();
  }, []);

  useEffect(() => {
    const opened: HTMLDetailsElement[] = [];
    const expand = (): void => {
      document.querySelectorAll<HTMLDetailsElement>("details:not([open])").forEach((element) => {
        element.open = true;
        opened.push(element);
      });
    };
    const restore = (): void => {
      opened.splice(0).forEach((element) => { element.open = false; });
    };
    window.addEventListener("beforeprint", expand);
    window.addEventListener("afterprint", restore);
    return () => {
      window.removeEventListener("beforeprint", expand);
      window.removeEventListener("afterprint", restore);
    };
  }, []);

  useEffect(() => {
    if (!stageFocusRequested.current) return;
    document.getElementById("stage-heading")?.focus();
    stageFocusRequested.current = false;
  }, [stage]);

  function moveTo(next: Stage): void {
    const index = STAGES.findIndex((item) => item.id === next);
    if (next === stage) {
      document.getElementById("stage-heading")?.focus();
    } else {
      stageFocusRequested.current = true;
      setStage(next);
    }
    setHighestStage((previous) => Math.max(previous, index));
    setError(null);
  }

  async function profileDataset(id: string): Promise<void> {
    await api.startProfile(id);
    const result = await api.getProfile(id);
    setProfile(result);
    setColumns(schemaFromProfile(result));
    setGlobalStatus(`${result.row_count.toLocaleString("ko-KR")}행, ${result.column_count}열 프로파일을 완료했습니다.`);
    moveTo("schema");
  }

  async function followInspection(id: string, state: string): Promise<void> {
    setDatasetId(id);
    if (state === "parse_options_required") {
      const response = await api.getParseOptions(id);
      if (response.confirmation) {
        setParseOptions(response.confirmation);
      } else {
        const recommendation = response.proposal?.recommended;
        setParseOptions((current) => ({
          ...current,
          encoding: recommendation?.encoding ?? response.proposal?.encoding ?? current.encoding,
          delimiter: recommendation?.delimiter ?? response.proposal?.delimiter ?? current.delimiter,
          quotechar: response.proposal?.quotechar ?? current.quotechar,
          escapechar: response.proposal?.escapechar ?? current.escapechar,
          has_header: response.proposal?.has_header ?? current.has_header,
          malformed: response.proposal?.malformed ?? current.malformed,
        }));
      }
      setMalformedPreview(response.malformed_preview);
      setUploadBranch("parse");
      setGlobalStatus("CSV 인코딩과 구분자 확인이 필요합니다.");
      return;
    }
    if (state === "sheet_required") {
      const response = await api.getSheets(id);
      setSheets(response.sheets);
      setSelectedSheet(response.selected_sheet ?? response.sheets[0]?.name ?? "");
      setUploadBranch("sheet");
      setGlobalStatus("처리할 XLSX 시트를 선택하세요.");
      return;
    }
    setUploadBranch("none");
    await profileDataset(id);
  }
  async function recoverWorkspace(): Promise<void> {
    const datasetList = await api.listDatasets(1);
    const recovered = datasetList.datasets[0];
    if (!recovered) {
      setGlobalStatus("준비되었습니다.");
      return;
    }
    setDatasetId(recovered.dataset_id);
    setDatasetManifestSha(recovered.manifest_sha256 ?? null);
    if (recovered.state === "uploading") {
      setRecoverableUpload(recovered);
      setUploadProgress({
        sent: recovered.upload_offset,
        total: recovered.size_bytes,
        phase: "uploading",
      });
      setGlobalStatus(
        `중단된 ${recovered.filename} 업로드가 있습니다. 같은 파일을 다시 선택하면 ${formatBytes(recovered.upload_offset)}부터 이어집니다.`,
      );
      return;
    }
    if (
      recovered.state === "parse_options_required" ||
      recovered.state === "sheet_required" ||
      recovered.state === "raw_ready"
    ) {
      await followInspection(recovered.dataset_id, recovered.state);
      return;
    }
    if (!["profiled", "schema_ready", "normalized"].includes(recovered.state)) {
      setGlobalStatus(`이전 데이터셋이 ${recovered.state} 상태에서 멈췄습니다.`);
      return;
    }
    const recoveredProfile = await api.getProfile(recovered.dataset_id);
    setProfile(recoveredProfile);
    if (recovered.state === "profiled") {
      setColumns(schemaFromProfile(recoveredProfile));
      setGlobalStatus("이전 프로파일 결과를 복원했습니다.");
      moveTo("schema");
      return;
    }
    const [persistedSchema, persistedRules] = await Promise.all([
      api.getSchema(recovered.dataset_id),
      api.getRules(recovered.dataset_id),
    ]);
    setColumns(persistedSchema.columns);
    setSchemaVersion(persistedSchema.schema_version);
    setRules(persistedRules.rules);
    setRulesVersion(persistedRules.rules_version);
    if (recovered.state === "schema_ready") {
      setGlobalStatus("저장된 스키마와 규칙을 복원했습니다.");
      moveTo("rules");
      return;
    }
    const jobList = await api.listJobs(recovered.dataset_id, 1);
    const recoveredJob = jobList.jobs[0];
    if (!recoveredJob) {
      setGlobalStatus("정규화된 데이터셋을 복원했습니다. 합성 설정을 계속할 수 있습니다.");
      moveTo("mode");
      return;
    }
    setJob(recoveredJob);
    setSynthesisMode(recoveredJob.mode);
    setJobProgress(recoveredJob.progress ?? null);
    setOutputRows(recoveredJob.output_rows);
    if (recoveredJob.state === "succeeded") {
      await loadReport(recoveredJob.job_id);
      return;
    }
    moveTo("progress");
    if (!["failed", "cancelled"].includes(recoveredJob.state)) {
      connectToJob(recoveredJob.job_id);
      setGlobalStatus("서버의 진행 중인 작업에 다시 연결했습니다.");
    } else {
      setGlobalStatus(`이전 작업의 ${recoveredJob.state} 상태를 복원했습니다.`);
    }
  }


  async function startUpload(): Promise<void> {
    if (!file) {
      setError("업로드할 CSV 또는 XLSX 파일을 선택하세요.");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const matchingUpload =
        recoverableUpload &&
        recoverableUpload.filename === file.name &&
        recoverableUpload.size_bytes === file.size
          ? recoverableUpload
          : undefined;
      const snapshot = await api.uploadFile(file, setUploadProgress, matchingUpload);
      setRecoverableUpload(null);
      await followInspection(snapshot.dataset_id, snapshot.state);
    } catch (uploadError) {
      setError(displayError(uploadError));
      setGlobalStatus("업로드를 완료하지 못했습니다. 서버 위치를 확인한 뒤 다시 시도할 수 있습니다.");
    } finally {
      setBusy(false);
    }
  }

  async function confirmParse(): Promise<void> {
    if (!datasetId) return;
    setBusy(true);
    setError(null);
    try {
      const snapshot = await api.confirmParseOptions(datasetId, parseOptions);
      await followInspection(datasetId, snapshot.state);
    } catch (parseError) {
      setError(displayError(parseError));
    } finally {
      setBusy(false);
    }
  }

  async function confirmSheet(): Promise<void> {
    if (!datasetId || !selectedSheet) return;
    setBusy(true);
    setError(null);
    try {
      const snapshot = await api.selectSheet(datasetId, selectedSheet);
      await followInspection(datasetId, snapshot.state);
    } catch (sheetError) {
      setError(displayError(sheetError));
    } finally {
      setBusy(false);
    }
  }

  function updateColumn(index: number, patch: Partial<ColumnSchema>): void {
    setColumns((current) =>
      current.map((column, columnIndex) => {
        if (index !== columnIndex) return column;
        const next = { ...column, ...patch };
        if (patch.kind === "identifier") {
          next.role = "identifier";
          next.identifier_strategy = next.identifier_strategy ?? "sequential";
        } else if (patch.kind === "excluded") {
          next.role = "excluded";
          delete next.identifier_strategy;
        } else if (patch.kind) {
          if (next.role === "identifier" || next.role === "excluded") next.role = "model";
          delete next.identifier_strategy;
        }
        if (patch.kind !== "fixed_decimal") delete next.decimal_places;
        if (patch.kind === "fixed_decimal" && next.decimal_places === undefined) next.decimal_places = 2;
        return next;
      }),
    );
  }

  async function saveSchema(): Promise<void> {
    if (!datasetId) return;
    setBusy(true);
    setError(null);
    try {
      const snapshot = await api.saveSchema(datasetId, columns);
      setSchemaVersion(snapshot.schema_version);
      setGlobalStatus("열 유형과 역할을 고정했습니다. 이제 무결성 규칙을 정의하세요.");
      moveTo("rules");
    } catch (schemaError) {
      setError(displayError(schemaError));
    } finally {
      setBusy(false);
    }
  }

  function addRule(): void {
    const validation = validateRuleDraft(ruleDraft);
    if (validation) {
      setRuleDraftError(validation);
      return;
    }
    const rule = makeRule(ruleDraft);
    setRules((current) => [...current, rule]);
    setRuleDraft((current) => ({ ...INITIAL_RULE_DRAFT, kind: current.kind }));
    setRuleDraftError(null);
  }

  async function saveRulesAndNormalize(): Promise<void> {
    if (!datasetId || conflicts.length > 0) return;
    setBusy(true);
    setError(null);
    try {
      const saved = await api.saveRules(datasetId, rules);
      setRulesVersion(saved.rules_version);
      await api.normalize(datasetId);
      const snapshot = await api.getDataset(datasetId);
      if (!snapshot.manifest_sha256) throw new Error("정규화된 데이터셋 manifest SHA가 없습니다.");
      setDatasetManifestSha(snapshot.manifest_sha256);
      setGlobalStatus(`${rules.length}개 규칙을 컴파일하고 정규화를 완료했습니다.`);
      moveTo("mode");
    } catch (rulesError) {
      setError(displayError(rulesError));
    } finally {
      setBusy(false);
    }
  }

  async function loadReport(jobId: string): Promise<void> {
    try {
      const artifactPayload = await api.getArtifacts(jobId);
      let reportPayload: PrimaryReport;
      try {
        reportPayload = await api.getPrimaryReport(jobId);
      } catch (primaryError) {
        if (!artifactPayload.artifacts.some((artifact) => artifact.kind === "dp_release_report_json")) {
          throw primaryError;
        }
        reportPayload = await api.getReleaseReport(jobId);
      }
      const evaluation = reportPayload.evaluation ?? reportPayload;
      setReport({
        ...evaluation,
        mode: reportPayload.mode ?? evaluation.mode ?? synthesisMode,
        ledger: reportPayload.ledger ?? evaluation.ledger,
        output: reportPayload.output ?? evaluation.output,
        release_safe: reportPayload.release_safe ?? evaluation.release_safe,
        contains_private_source_information:
          reportPayload.contains_private_source_information
          ?? evaluation.contains_private_source_information,
        narrative: reportPayload.narrative ?? evaluation.narrative,
        executive_summary: reportPayload.executive_summary ?? evaluation.executive_summary,
      });
      setArtifacts(artifactPayload.artifacts);
      setGlobalStatus("합성 데이터와 담당자용 종합 보고서가 준비되었습니다.");
      moveTo("report");
    } catch (reportError) {
      setError(displayError(reportError));
    }
  }

  function connectToJob(jobId: string): void {
    eventSource.current?.close();
    const source = new EventSource(`/api/v1/jobs/${jobId}/events`);
    eventSource.current = source;
    let terminalSeen = false;
    const handleEvent = (message: MessageEvent<string>): void => {
      try {
        const payload = JSON.parse(message.data) as ProgressEventPayload;
        setJobProgress(payload);
        setGlobalStatus(`${JOB_STAGE_LABELS[payload.stage] ?? payload.stage}: ${payload.completed}/${payload.total} ${payload.unit ?? "단계"}`);
        if (payload.state === "succeeded") {
          terminalSeen = true;
          source.close();
          void loadReport(jobId);
        } else if (payload.state === "cancelled" || payload.state === "failed") {
          terminalSeen = true;
          source.close();
          void api.getJob(jobId).then(setJob).catch((jobError: unknown) => setError(displayError(jobError)));
        }
      } catch {
        setError("진행 이벤트 형식을 읽지 못했습니다.");
      }
    };
    source.onmessage = handleEvent;
    source.addEventListener("progress", handleEvent as EventListener);
    source.addEventListener("terminal", handleEvent as EventListener);
    source.onerror = () => {
      if (!terminalSeen) {
        setGlobalStatus("진행 연결을 복구하고 있습니다. 완료된 이벤트는 서버에서 다시 재생됩니다.");
      }
    };
  }

  async function publishPublicMetadata(file: File): Promise<void> {
    setBusy(true);
    setError(null);
    try {
      const parsed = JSON.parse(await file.text()) as Record<string, unknown>;
      const manifest = await api.publishPublicMetadata(parsed);
      setPublicMetadata(manifest);
      setPublicMetadataName(file.name);
      setGlobalStatus("공개 메타데이터를 검증하고 고정했습니다.");
    } catch (metadataError) {
      setPublicMetadata(null);
      setPublicMetadataName("");
      setError(displayError(metadataError));
    } finally {
      setBusy(false);
    }
  }

  async function createJob(): Promise<void> {
    if (!datasetId || !datasetManifestSha || outputFormats.length === 0) {
      setError("정규화된 데이터셋과 한 개 이상의 출력 형식이 필요합니다.");
      return;
    }
    let request: SynthesisRequest;
    if (synthesisMode === "differential_privacy") {
      if (!publicMetadata) {
        setError("형식적 DP에는 검증된 공개 메타데이터 JSON이 필요합니다.");
        return;
      }
      request = {
        version: "1.0",
        dataset_id: datasetId,
        dataset_manifest_sha: datasetManifestSha,
        schema_version: schemaVersion,
        rules_version: rulesVersion,
        mode: "differential_privacy",
        synthesizer: "mst",
        output_rows: outputRows,
        output_formats: outputFormats,
        resource_profile: resourceProfile,
        evaluation_config_version: "1.0",
        privacy: {
          adjacency: "add_remove_one_row",
          privacy_unit: "row",
          epsilon_model: epsilonModel,
          delta,
          epsilon_preprocess: 0,
          public_metadata_manifest: publicMetadata,
          public_target_count: outputRows,
          fit_sampling_rate: fitSamplingRate,
          sampling_seed: samplingSeed,
        },
      } satisfies DifferentialPrivacySynthesisRequest;
    } else {
      request = {
        version: "1.0",
        dataset_id: datasetId,
        dataset_manifest_sha: datasetManifestSha,
        schema_version: schemaVersion,
        rules_version: rulesVersion,
        mode: "utility",
        synthesizer: "tabular_argn",
        output_rows: outputRows,
        output_formats: outputFormats,
        resource_profile: resourceProfile,
        evaluation_config_version: "1.0",
        generation_seed: generationSeed,
        training: {
          max_rows: trainingRows,
          max_epochs: maxEpochs,
          max_minutes: maxMinutes,
          model_size: modelSize,
          device,
        },
      } satisfies UtilitySynthesisRequest;
    }
    setBusy(true);
    setError(null);
    setReport(null);
    setArtifacts([]);
    setHighestStage((current) => Math.min(current, 4));
    try {
      const snapshot = await api.createJob(request);
      setJob(snapshot);
      setJobProgress(snapshot.progress ?? null);
      moveTo("progress");
      if (snapshot.state === "succeeded") {
        await loadReport(snapshot.job_id);
      } else {
        connectToJob(snapshot.job_id);
      }
    } catch (jobError) {
      setError(displayError(jobError));
    } finally {
      setBusy(false);
    }
  }

  async function cancelJob(): Promise<void> {
    if (!job) return;
    setBusy(true);
    setError(null);
    try {
      const snapshot = await api.cancelJob(job.job_id);
      setJob(snapshot);
      setGlobalStatus("취소 요청을 전달했습니다. 현재 batch 경계에서 안전하게 멈춥니다.");
    } catch (cancelError) {
      setError(displayError(cancelError));
    } finally {
      setBusy(false);
    }
  }

  async function resumeJob(): Promise<void> {
    if (!job) return;
    setBusy(true);
    setError(null);
    try {
      const snapshot = await api.resumeJob(job.job_id);
      setJob(snapshot);
      setJobProgress(snapshot.progress ?? null);
      setGlobalStatus("새 작업 ID로 마지막 검증 경계부터 재개했습니다.");
      connectToJob(snapshot.job_id);
    } catch (resumeError) {
      setError(displayError(resumeError));
    } finally {
      setBusy(false);
    }
  }

  function navigateReportTabs(event: KeyboardEvent<HTMLButtonElement>, current: ReportTab): void {
    const tabs: ReportTab[] = ["summary", "columns", "boundary"];
    let next: ReportTab | undefined;
    if (event.key === "ArrowRight") {
      next = tabs[(tabs.indexOf(current) + 1) % tabs.length];
    } else if (event.key === "ArrowLeft") {
      next = tabs[(tabs.indexOf(current) - 1 + tabs.length) % tabs.length];
    } else if (event.key === "Home") {
      next = tabs[0];
    } else if (event.key === "End") {
      next = tabs.at(-1);
    }
    if (!next) return;
    event.preventDefault();
    setReportTab(next);
    document.getElementById(`tab-${next}`)?.focus();
  }

  const uploadPercent = uploadProgress
    ? Math.round((uploadProgress.sent / Math.max(uploadProgress.total, 1)) * 100)
    : 0;
  const jobPercent = jobProgress
    ? Math.round((jobProgress.completed / Math.max(jobProgress.total, 1)) * 100)
    : 0;

  return (
    <div className="app-shell">
      <a className="skip-link" href="#main-content">본문으로 건너뛰기 / Skip to main content</a>

      <header className="site-header">
        <h1 className="site-name">Synthetic Table Studio</h1>
      </header>

      <nav className="step-rail" aria-label="합성 데이터 생성 단계">
        <ol>
          {STAGES.map((item, index) => (
            <li key={item.id} data-state={index < currentStageIndex ? "complete" : index === currentStageIndex ? "current" : "upcoming"}>
              <button
                type="button"
                disabled={index > highestStage}
                aria-current={index === currentStageIndex ? "step" : undefined}
                onClick={() => moveTo(item.id)}
              >
                <span className="step-number" aria-hidden="true">{index < currentStageIndex ? "✓" : index + 1}</span>
                <span><strong>{item.label}</strong><small>{item.hint}</small></span>
              </button>
            </li>
          ))}
        </ol>
      </nav>

      <main id="main-content" className="main-content" tabIndex={-1}>

        <div className="global-status" role="status" aria-live="polite">
          <span aria-hidden="true">{error ? "!" : sessionReady ? "✓" : "…"}</span>
          <span>{globalStatus}</span>
        </div>
        {error && <div className="error-banner" role="alert"><strong>처리할 수 없음</strong><span>{error}</span></div>}

        <section className="stage-panel" aria-labelledby="stage-heading">
          {stage === "upload" && (
            <>
              <div className="stage-heading-row">
                <div><p className="section-label">01 · Source</p><h2 id="stage-heading" tabIndex={-1}>원본 파일 업로드</h2></div>
                <p className="stage-note">최대 8 GiB · CSV / XLSX</p>
              </div>
              <p className="lead-copy">파일은 64 MiB 이하 조각으로 이 브라우저에서 localhost로 직접 전송됩니다. 파일 전체를 브라우저 메모리에 올리지 않습니다.</p>

              <div className="upload-zone">
                <label htmlFor="source-file">CSV 또는 XLSX 파일</label>
                <input
                  id="source-file"
                  type="file"
                  accept=".csv,.xlsx,text/csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                  disabled={busy}
                  onChange={(event) => {
                    setFile(event.target.files?.[0] ?? null);
                    setUploadProgress(null);
                    setUploadBranch("none");
                    setError(null);
                  }}
                />
                <div className="file-summary" aria-live="polite">
                  {file ? <><strong>{file.name}</strong><span>{formatBytes(file.size)}</span></> : <span>파일을 선택하면 이름과 크기를 먼저 확인합니다.</span>}
                </div>
                <button className="button primary" type="button" disabled={!file || busy || !sessionReady} onClick={() => void startUpload()}>
                  {busy ? "안전하게 전송 중…" : "업로드하고 검사"}
                </button>
              </div>

              {uploadProgress && (
                <div className="progress-block" aria-live="polite">
                  <div className="progress-copy"><strong>{uploadProgress.phase === "hashing" ? "체크섬 계산" : uploadProgress.phase === "uploading" ? "조각 전송" : "형식 검사"}</strong><span>{uploadPercent}%</span></div>
                  <progress max={100} value={uploadPercent} aria-label="파일 업로드 진행률">{uploadPercent}%</progress>
                  <p>{formatBytes(uploadProgress.sent)} / {formatBytes(uploadProgress.total)}</p>
                </div>
              )}

              {uploadBranch === "parse" && (
                <form className="confirmation-panel" onSubmit={(event) => { event.preventDefault(); void confirmParse(); }}>
                  <div><p className="section-label">Confirmation required</p><h3>CSV 읽기 방식 확인</h3></div>
                  <div className="form-grid three">
                    <label>인코딩<select value={parseOptions.encoding} onChange={(event) => setParseOptions({ ...parseOptions, encoding: event.target.value as ParseOptions["encoding"] })}><option value="utf-8">UTF-8</option><option value="utf-8-sig">UTF-8 BOM</option><option value="cp949">CP949</option><option value="euc-kr">EUC-KR</option></select></label>
                    <label>구분자<input value={parseOptions.delimiter} maxLength={1} onChange={(event) => setParseOptions({ ...parseOptions, delimiter: event.target.value })} /></label>
                    <label>잘못된 레코드<select value={parseOptions.malformed} onChange={(event) => setParseOptions({ ...parseOptions, malformed: event.target.value as ParseOptions["malformed"] })}><option value="fail">중단</option><option value="skip">건너뛰고 비공개 보고</option></select></label>
                  </div>
                  <label className="checkbox-row"><input type="checkbox" checked={parseOptions.has_header} onChange={(event) => setParseOptions({ ...parseOptions, has_header: event.target.checked })} />첫 행을 열 이름으로 사용</label>
                  {malformedPreview.length > 0 && <p className="warning-copy"><strong>미리보기 경고:</strong> 해석하기 어려운 레코드 {malformedPreview.length}개가 표본에서 발견되었습니다.</p>}
                  <button className="button primary" type="submit" disabled={busy}>이 방식으로 계속</button>
                </form>
              )}

              {uploadBranch === "sheet" && (
                <form className="confirmation-panel" onSubmit={(event) => { event.preventDefault(); void confirmSheet(); }}>
                  <div><p className="section-label">Confirmation required</p><h3>XLSX 시트 선택</h3></div>
                  <label>처리할 시트<select value={selectedSheet} onChange={(event) => setSelectedSheet(event.target.value)}>{sheets.map((sheet) => <option key={sheet.name} value={sheet.name}>{sheet.name}{sheet.rows !== undefined ? ` · ${sheet.rows.toLocaleString("ko-KR")}행` : ""}</option>)}</select></label>
                  <p className="support-copy">한 번에 한 시트만 처리합니다. 숨김 시트와 수식·외부 연결은 안전 검사를 통과해야 합니다.</p>
                  <button className="button primary" type="submit" disabled={!selectedSheet || busy}>이 시트로 계속</button>
                </form>
              )}
            </>
          )}

          {stage === "schema" && profile && (
            <>
              <div className="stage-heading-row"><div><p className="section-label">02 · Meaning</p><h2 id="stage-heading" tabIndex={-1}>열 스키마 확인</h2></div><p className="stage-note">{profile.row_count.toLocaleString("ko-KR")}행 · {profile.column_count}열</p></div>
              <p className="lead-copy">자동 제안은 확정값이 아닙니다. 숫자로만 보이는 코드와 식별자는 반드시 의미에 맞게 바꾸세요.</p>
              <div className="schema-toolbar">
                <label>열 검색<input type="search" value={schemaQuery} onChange={(event) => setSchemaQuery(event.target.value)} placeholder="열 이름" /></label>
                <label className="checkbox-row"><input type="checkbox" checked={schemaNeedsReviewOnly} onChange={(event) => setSchemaNeedsReviewOnly(event.target.checked)} />확인이 필요한 열만</label>
                <span role="status">{visibleColumns.length} / {columns.length}열 표시</span>
              </div>
              <div className="table-wrap">
                <table className="schema-table">
                  <caption className="visually-hidden">열 이름, 프로파일, 유형, 역할, 결측 허용 편집</caption>
                  <thead><tr><th scope="col">열 / 관찰값</th><th scope="col">유형</th><th scope="col">역할</th><th scope="col">결측</th></tr></thead>
                  <tbody>{visibleColumns.map(({ column, index, profile: observed }) => {
                    return <tr key={column.name}>
                      <th scope="row"><strong>{column.name}</strong><small>{observed?.approx_cardinality.toLocaleString("ko-KR")}개 고유값 추정 · null {observed?.null_count.toLocaleString("ko-KR")}</small>{observed?.candidate_requires_confirmation && <span className="inline-warning">확인 필요</span>}</th>
                      <td><label className="cell-label"><span className="visually-hidden">{column.name} 유형</span><select value={column.kind} onChange={(event) => updateColumn(index, { kind: event.target.value as ColumnKind })}>{Object.entries(KIND_LABELS).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>{column.kind === "fixed_decimal" && <label className="compact-field">소수 자릿수<input type="number" min={0} max={18} value={column.decimal_places ?? 2} onChange={(event) => updateColumn(index, { decimal_places: Number(event.target.value) })} /></label>}</td>
                      <td><label className="cell-label"><span className="visually-hidden">{column.name} 역할</span><select value={column.role} disabled={column.kind === "identifier" || column.kind === "excluded"} onChange={(event) => updateColumn(index, { role: event.target.value as ColumnRole })}>{Object.entries(ROLE_LABELS).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label></td>
                      <td><label className="checkbox-row compact"><input type="checkbox" checked={column.nullable} onChange={(event) => updateColumn(index, { nullable: event.target.checked })} /><span>허용</span></label></td>
                    </tr>;
                  })}</tbody>
                </table>
              </div>
              <div className="action-row"><button className="button primary" type="button" disabled={busy} onClick={() => void saveSchema()}>스키마 저장</button></div>
            </>
          )}

          {stage === "rules" && (
            <>
              <div className="stage-heading-row"><div><p className="section-label">03 · Integrity</p><h2 id="stage-heading" tabIndex={-1}>무결성 규칙 정의</h2></div><p className="stage-note">{rules.length}개 규칙</p></div>
              <p className="lead-copy">규칙은 화면 순서가 아니라 검증된 읽기/쓰기 그래프에 따라 적용됩니다. 동일 열의 다중 writer와 순환은 저장 전에 차단합니다.</p>
              <div className="rule-layout">
                <form className="rule-builder" onSubmit={(event) => { event.preventDefault(); addRule(); }}>
                  <label>규칙 형태<select value={ruleDraft.kind} onChange={(event) => { setRuleDraft({ ...INITIAL_RULE_DRAFT, kind: event.target.value as RuleKind }); setRuleDraftError(null); }}>{Object.entries(RULE_LABELS).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
                  <RuleFields draft={ruleDraft} columns={columns} onChange={setRuleDraft} />
                  <div className="form-grid two">
                    <label>근거<select value={ruleDraft.provenance} onChange={(event) => setRuleDraft({ ...ruleDraft, provenance: event.target.value as RuleDraft["provenance"] })}><option value="public">공개 규칙</option><option value="private_inferred">원본에서 추론</option></select></label>
                    <label>원본 위반 시<select value={ruleDraft.sourceAction} onChange={(event) => setRuleDraft({ ...ruleDraft, sourceAction: event.target.value as RuleDraft["sourceAction"] })}><option value="block">작업 중단</option><option value="drop_row">위반 행 제외</option></select></label>
                  </div>
                  {ruleDraftError && <p className="field-error" role="alert">{ruleDraftError}</p>}
                  <button className="button secondary" type="submit">규칙 추가</button>
                </form>
                <div className="rule-list" aria-live="polite">
                  <h3>컴파일할 규칙</h3>
                  {rules.length === 0 ? <p className="empty-note">규칙이 없어도 진행할 수 있습니다. 출력은 스키마만 검증합니다.</p> : <ol>{rules.map((rule) => <li key={rule.id}><div><strong>{RULE_LABELS[rule.kind]}</strong><small>{describeRule(rule)}</small></div><button className="text-button" type="button" aria-label={`${RULE_LABELS[rule.kind]} 규칙 삭제`} onClick={() => setRules((current) => current.filter((item) => item.id !== rule.id))}>삭제</button></li>)}</ol>}
                </div>
              </div>
              {conflicts.length > 0 && <div className="conflict-banner" role="alert"><strong>규칙 충돌 {conflicts.length}건</strong><ul>{conflicts.map((conflict) => <li key={conflict}>{conflict}</li>)}</ul></div>}
              <div className="action-row"><button className="button primary" type="button" disabled={busy || conflicts.length > 0} onClick={() => void saveRulesAndNormalize()}>규칙 저장하고 정규화</button></div>
            </>
          )}

          {stage === "mode" && (
            <>
              <div className="stage-heading-row"><div><p className="section-label">04 · Method</p><h2 id="stage-heading" tabIndex={-1}>생성 모드와 자원</h2></div><p className="stage-note">{resourcePlan ? `${resourcePlan.resource_profile} · 자동 설정` : "시스템 자원 확인 중"}</p></div>
              <div className="mode-grid" role="radiogroup" aria-label="합성 모드">
                <label className={`mode-card${synthesisMode === "utility" ? " selected" : ""}`}>
                  <input type="radio" name="mode" value="utility" checked={synthesisMode === "utility"} onChange={() => setSynthesisMode("utility")} />
                  <span className="mode-tag">사용 가능</span>
                  <strong>일반 고품질 합성</strong>
                  <span>TabularARGN · 관계와 조건부 분포 중심</span>
                  <em>원본의 분포와 열 관계를 학습하지만, ε/δ로 보정된 수학적 보호 보장은 없습니다.</em>
                </label>
                <label className={`mode-card${synthesisMode === "differential_privacy" ? " selected" : ""}`} aria-describedby="dp-audit dp-boundary">
                  <input type="radio" name="mode" value="differential_privacy" checked={synthesisMode === "differential_privacy"} onChange={() => setSynthesisMode("differential_privacy")} />
                  <span className="mode-tag">검증된 MST 실행 경로</span>
                  <strong>형식적 차등프라이버시</strong>
                  <span>MST · 행 단위 add/remove 인접성</span>
                  <em id="dp-audit">checkpoint는 trusted curator 내부에만 보관합니다. 공개 메타데이터로 이산화하고 별도 프로세스에서 공개 sampling seed로 결과를 생성합니다.</em>
                </label>
              </div>
              <div id="dp-boundary" className="boundary-note">
                <strong>DP 공개 경계란?</strong>
                <p>원본을 볼 수 있는 내부 작업 영역과 외부로 내보낼 수 있는 산출물 사이의 선입니다. 형식적 DP에서는 DP 메커니즘의 결과와 사전에 공개한 메타데이터만 경계 밖으로 나갑니다.</p>
                <dl>
                  <div><dt>경계 안</dt><dd>원본, 비공개 프로파일, 학습 체크포인트, 내부 진단 보고서</dd></div>
                  <div><dt>경계 밖</dt><dd>release_safe=true이고 원본 정보를 포함하지 않는 결과와 공개 보고서만 허용</dd></div>
                  <div><dt>지원 범위</dt><dd>공개 범위·범주가 정해진 모델링 열 최대 32개와 공개 수식 파생 열</dd></div>
                  <div><dt>현재 상태</dt><dd>MST fit/sample 경로와 privacy ledger, 공개 산출물 allowlist가 활성화됨</dd></div>
                </dl>
              </div>
              {hostResources && resourcePlan && (
                <aside className="guidance-panel" aria-labelledby="resource-summary-heading">
                  <h3 id="resource-summary-heading">현재 시스템에 맞춘 실행 설정</h3>
                  <ul>
                    <li><strong>CPU</strong><span>{hostResources.logical_cpu_count.toLocaleString("ko-KR")} logical cores</span></li>
                    <li><strong>메모리</strong><span>전체 {formatGiB(hostResources.total_memory_bytes)} · 현재 사용 가능 {formatGiB(hostResources.available_memory_bytes)}</span></li>
                    <li><strong>GPU</strong><span>{hostResources.gpu_backend === "none" ? "감지되지 않음" : `${hostResources.gpu_name ?? hostResources.gpu_backend} · ${resourcePlan.recommended_device}`}</span></li>
                    <li><strong>디스크</strong><span>사용 가능 {formatGiB(hostResources.disk_free_bytes)}</span></li>
                    <li><strong>작업 한도</strong><span>worker {formatGiB(resourcePlan.worker_lease_bytes)} · 동시 작업 {resourcePlan.max_concurrent_jobs}개 · DuckDB {formatGiB(resourcePlan.duckdb_memory_limit_bytes)}</span></li>
                  </ul>
                  <p className="support-copy">작업을 시작할 때 예상 산출물과 현재 디스크 여유를 다시 비교합니다. GPU는 현재 호스트와 검증된 backend gate가 모두 일치할 때만 자동 선택합니다.</p>
                </aside>
              )}
              {synthesisMode === "utility" && <aside className="guidance-panel" aria-labelledby="training-guidance-heading">
                <h3 id="training-guidance-heading">권장 학습 설정</h3>
                <ul><li><strong>1 epoch</strong><span>빠른 동작 확인</span></li><li><strong>5 epoch</strong><span>첫 품질 비교 권장값</span></li><li><strong>Small 모델</strong><span>M4에서 검증된 크기</span></li></ul>
              </aside>}
              <fieldset className="resource-form">
                <legend>{synthesisMode === "utility" ? "일반 합성 실행 설정" : "형식적 DP 실행 설정"}</legend>
                <div className="form-grid three">
                  <label>출력 행 수<input type="number" min={1} max={55_000_000} value={outputRows} onChange={(event) => setOutputRows(Number(event.target.value))} /></label>
                  <label>자원 프로필<input value={resourceProfile} readOnly aria-readonly="true" /></label>
                  {synthesisMode === "utility" ? <>
                    <label>학습 표본 상한<input type="number" min={1} max={resourcePlan?.utility_max_rows ?? 250_000} value={trainingRows} onChange={(event) => setTrainingRows(Number(event.target.value))} /></label>
                    <label>최대 epoch<input type="number" min={1} max={100} value={maxEpochs} onChange={(event) => setMaxEpochs(Number(event.target.value))} /></label>
                    <label>최대 학습 시간 (분)<input type="number" min={1} max={1440} value={maxMinutes} onChange={(event) => setMaxMinutes(Number(event.target.value))} /></label>
                    <label>모델 크기<select value={modelSize} onChange={(event) => setModelSize(event.target.value)}><option value="small">Small · 검증됨</option><option value="medium" disabled>Medium · 미검증</option></select></label>
                    <label>실행 장치<select value={device} onChange={(event) => setDevice(event.target.value)}><option value="cpu">CPU</option>{resourcePlan?.recommended_device === "mps" && <option value="mps">Apple MPS · 현재 호스트 검증됨</option>}{resourcePlan?.recommended_device === "cuda:0" && <option value="cuda:0">CUDA 0 · 현재 호스트 검증됨</option>}</select></label>
                    <label>생성 seed<input type="number" min={0} max={4_294_967_295} value={generationSeed} onChange={(event) => setGenerationSeed(Number(event.target.value))} /></label>
                  </> : <>
                    <label>ε model<input value={epsilonModel} inputMode="decimal" onChange={(event) => setEpsilonModel(event.target.value)} /></label>
                    <label>δ<input value={delta} inputMode="decimal" onChange={(event) => setDelta(event.target.value)} /></label>
                    <label>공개 fit sampling rate<input value={fitSamplingRate} inputMode="decimal" onChange={(event) => setFitSamplingRate(event.target.value)} /></label>
                    <label>공개 sampling seed<input type="number" min={0} value={samplingSeed} onChange={(event) => setSamplingSeed(Number(event.target.value))} /></label>
                    <label>공개 메타데이터 JSON<input type="file" accept=".json,application/json" disabled={busy} onChange={(event) => { const selected = event.target.files?.[0]; if (selected) void publishPublicMetadata(selected); }} /><small>{publicMetadata ? `${publicMetadataName} · 검증됨` : "공개 provenance, 범주·bin, 공개 규칙 hash 필요"}</small></label>
                  </>}
                </div>
                <fieldset className="format-fieldset">
                  <legend>출력 형식</legend>
                  <label className="checkbox-row"><input type="checkbox" checked={outputFormats.includes("parquet")} onChange={(event) => setOutputFormats((current) => event.target.checked ? [...new Set([...current, "parquet" as const])] : current.filter((value) => value !== "parquet"))} />Parquet shards + ZIP64 manifest</label>
                  <label className="checkbox-row"><input type="checkbox" checked={outputFormats.includes("csv")} onChange={(event) => setOutputFormats((current) => event.target.checked ? [...new Set([...current, "csv" as const])] : current.filter((value) => value !== "csv"))} />CSV</label>
                </fieldset>
              </fieldset>
              <div className="action-row"><p>시작 전 디스크·RAM admission과 공개 경계를 통과하지 못하면 산출물을 만들지 않습니다.</p><button className="button primary" type="button" disabled={busy || outputFormats.length === 0 || (synthesisMode === "differential_privacy" && !publicMetadata)} onClick={() => void createJob()}>{synthesisMode === "utility" ? "일반 합성 시작" : "형식적 DP 합성 시작"}</button></div>
            </>
          )}

          {stage === "progress" && job && (
            <>
              <div className="stage-heading-row"><div><p className="section-label">05 · Execution</p><h2 id="stage-heading" tabIndex={-1}>합성 작업 진행</h2></div><p className="stage-note">시도 {job.attempt ?? 1}</p></div>
              <div className="job-overview" role="status" aria-live="polite" aria-atomic="true"><div><span className="status-symbol" aria-hidden="true">{job.state === "cancelled" || job.state === "failed" ? "!" : "↻"}</span><div><strong>{JOB_STAGE_LABELS[jobProgress?.stage ?? job.state] ?? job.state}</strong><span>작업 ID {job.job_id}</span></div></div><strong>{jobPercent}%</strong></div>
              <progress className="job-progress" max={100} value={jobPercent} aria-label="합성 작업 진행률" aria-valuetext={`${JOB_STAGE_LABELS[jobProgress?.stage ?? job.state] ?? job.state} ${jobPercent}%`}>{jobPercent}%</progress>
              <ol className="pipeline-list" aria-label="서버 처리 단계">{["preparing", "fitting", "generating", "repairing", "evaluating", "exporting", "publishing"].map((pipelineStage) => { const known = Object.keys(JOB_STAGE_LABELS).indexOf(jobProgress?.stage ?? job.state); const index = Object.keys(JOB_STAGE_LABELS).indexOf(pipelineStage); return <li key={pipelineStage} data-state={pipelineStage === (jobProgress?.stage ?? job.state) ? "current" : index < known ? "complete" : "upcoming"}><span aria-hidden="true">{index < known ? "✓" : pipelineStage === (jobProgress?.stage ?? job.state) ? "→" : "·"}</span>{JOB_STAGE_LABELS[pipelineStage]}</li>; })}</ol>
              <div className="action-row"><p>페이지를 다시 열어도 retained SSE 이벤트를 마지막 ID부터 재생할 수 있습니다.</p>{job.legal_actions?.includes("resume") || job.state === "cancelled" ? <button className="button primary" type="button" disabled={busy} onClick={() => void resumeJob()}>새 작업으로 재개</button> : <button className="button danger" type="button" disabled={busy || ["succeeded", "failed"].includes(job.state)} onClick={() => void cancelJob()}>작업 취소</button>}</div>
            </>
          )}

          {stage === "report" && report && job && (
            <>
              <div className="stage-heading-row"><div><p className="section-label">06 · Evidence</p><h2 id="stage-heading" tabIndex={-1}>품질 보고서와 산출물</h2></div><p className="stage-note">완료 · {report.mode === "differential_privacy" ? "formal DP · 담당자용 종합 분석" : "utility"}</p></div>
              {report.mode === "differential_privacy" && report.release_safe !== true
                ? <div className="report-callout"><span aria-hidden="true">!</span><p><strong>담당자 내부 보고서</strong>원본·holdout 기반 유사도와 경험적 프라이버시 진단을 모두 포함합니다. 형식적 DP가 적용된 결과지만 이 보고서 자체는 원본 파생 정보를 포함하므로 외부 공개용이 아닙니다.</p></div>
                : report.mode === "differential_privacy"
                  ? <div className="report-callout success"><span aria-hidden="true">✓</span><p><strong>DP 공개 경계 통과</strong>이 보고서는 release_safe 공개 정보만 포함합니다. 형식적 보장의 범위는 ε, δ, 인접성, privacy unit을 확인하세요.</p></div>
                  : <div className="report-callout"><span aria-hidden="true">!</span><p><strong>개인정보 보호 보장 없음</strong>이 일반 합성 결과와 품질 보고서는 형식적 DP 공개 산출물이 아닙니다. 다운로드 전에 조직의 공개 기준을 별도로 검토하세요.</p></div>}
              <ReportVerdict report={report} artifacts={artifacts} />
              <div className="report-tabs">
                <div role="tablist" aria-label="보고서 보기">{(["summary", "columns", "boundary"] as ReportTab[]).map((tab) => <button key={tab} type="button" role="tab" aria-selected={reportTab === tab} aria-controls={`report-${tab}`} id={`tab-${tab}`} tabIndex={reportTab === tab ? 0 : -1} onClick={() => setReportTab(tab)} onKeyDown={(event) => navigateReportTabs(event, tab)}>{tab === "summary" ? "품질 요약" : tab === "columns" ? "열별 거리" : "공개 경계"}</button>)}</div>
                <div role="tabpanel" tabIndex={0} hidden={reportTab !== "summary"} id="report-summary" aria-labelledby="tab-summary"><ReportExecutiveSummary report={report} />{report.mode === "differential_privacy" && <DpReleaseSummary report={report} />}<ReportSummary report={report} /><AdvancedEvaluation report={report} /><Suspense fallback={<p className="empty-note" role="status">차트를 불러오는 중…</p>}><ReportChart report={report} /></Suspense><ReportGlossary /></div>
                <div role="tabpanel" tabIndex={0} hidden={reportTab !== "columns"} id="report-columns" aria-labelledby="tab-columns">{report.mode === "differential_privacy" && report.release_safe === true ? <p className="empty-note">DP 공개 보고서는 원본과 비교한 열별 private-source 지표를 공개하지 않습니다.</p> : <ReportColumns report={report} />}</div>
                <div role="tabpanel" tabIndex={0} hidden={reportTab !== "boundary"} id="report-boundary" aria-labelledby="tab-boundary">{report.mode === "differential_privacy" && report.release_safe !== true ? <div className="boundary-note utility"><strong>담당자 내부 DP 분석 경계</strong><p>이 화면과 primary_report 파일은 원본·holdout 유사도, C2ST, Gower 및 공격 진단을 포함하므로 외부 공개할 수 없습니다. 별도의 dp_release_report만 release_safe 공개 경계를 통과합니다.</p></div> : report.mode === "differential_privacy" ? <div className="boundary-note"><strong>Formal DP release 경계</strong><p>checkpoint, 원본 프로파일, holdout, 공격 진단은 경계 안에 남습니다. release_safe=true이고 원본 정보가 없는 산출물만 외부 공개 후보입니다.</p></div> : <div className="boundary-note utility"><strong>Utility / curator 경계</strong><p>다운로드 가능 여부는 localhost 사용자의 파일 접근 권한일 뿐 외부 공개 승인이 아닙니다. 원본 프로파일, holdout 품질, 공격 진단과 일부 보고서는 private source information을 포함할 수 있습니다. release_safe=false 산출물은 조직의 별도 검토 없이 공개하지 마세요.</p></div>}</div>
              </div>
              <ArtifactDownloads artifacts={artifacts} legacyReport={!report.executive_summary} />
            </>
          )}
        </section>
      </main>

    </div>
  );
}

function ColumnSelect({ label, value, columns, onChange }: { label: string; value: string; columns: ColumnSchema[]; onChange: (value: string) => void }) {
  return <label>{label}<select value={value} onChange={(event) => onChange(event.target.value)}><option value="">열 선택</option>{columns.filter((column) => column.kind !== "excluded").map((column) => <option key={column.name} value={column.name}>{column.name}</option>)}</select></label>;
}

function RuleFields({ draft, columns, onChange }: { draft: RuleDraft; columns: ColumnSchema[]; onChange: (draft: RuleDraft) => void }) {
  const set = (patch: Partial<RuleDraft>) => onChange({ ...draft, ...patch });
  if (draft.kind === "fixed_combination") return <><label>조합 열 (쉼표 구분)<input value={draft.values} placeholder="지역코드, 지점코드" onChange={(event) => set({ values: event.target.value })} /></label><label>공개 허용 tuple (선택, 세미콜론으로 행 구분)<textarea value={draft.tuples} placeholder="서울,001; 부산,002" onChange={(event) => set({ tuples: event.target.value })} /></label></>;
  if (draft.kind === "sum_equals") return <><label>합산 원본 열 (쉼표 구분)<input value={draft.values} placeholder="기본급, 수당" onChange={(event) => set({ values: event.target.value })} /></label><ColumnSelect label="합계 대상 열" value={draft.secondary} columns={columns} onChange={(value) => set({ secondary: value })} /><label>허용 오차<input value={draft.tolerance} inputMode="decimal" onChange={(event) => set({ tolerance: event.target.value })} /></label></>;
  if (draft.kind === "conditional_set") return <><ColumnSelect label="조건 열" value={draft.column} columns={columns} onChange={(value) => set({ column: value })} /><div className="form-grid two"><label>조건<select value={draft.operator} onChange={(event) => set({ operator: event.target.value })}><option value="=">같음</option><option value="!=">다름</option><option value="is_null">null임</option></select></label><label>조건 값<input value={draft.value} disabled={draft.operator === "is_null"} onChange={(event) => set({ value: event.target.value })} /></label></div><ColumnSelect label="고정할 대상 열" value={draft.secondary} columns={columns} onChange={(value) => set({ secondary: value })} /><label>고정값<input value={draft.tertiary} onChange={(event) => set({ tertiary: event.target.value })} /></label></>;
  if (draft.kind === "compare") return <><ColumnSelect label="왼쪽 열" value={draft.column} columns={columns} onChange={(value) => set({ column: value })} /><label>비교<select value={draft.operator === "=" ? "<=" : draft.operator} onChange={(event) => set({ operator: event.target.value })}><option value="<">작음 (&lt;)</option><option value="<=">작거나 같음 (≤)</option><option value=">">큼 (&gt;)</option><option value=">=">크거나 같음 (≥)</option></select></label><ColumnSelect label="오른쪽 열" value={draft.secondary} columns={columns} onChange={(value) => set({ secondary: value })} /><label>공개 단위 / granularity (선택)<input value={draft.value} onChange={(event) => set({ value: event.target.value })} /></label></>;
  return <><ColumnSelect label="대상 열" value={draft.column} columns={columns} onChange={(value) => set({ column: value })} />{draft.kind === "mask_prefix" && <label>유지할 앞 문자 수<input type="number" min={0} value={draft.keepChars} onChange={(event) => set({ keepChars: event.target.value })} /></label>}{draft.kind === "allowed_values" && <label>허용값 (쉼표 구분)<input value={draft.values} placeholder="A, B, C" onChange={(event) => set({ values: event.target.value })} /></label>}{draft.kind === "range" && <div className="form-grid two"><label>최솟값<input value={draft.min} onChange={(event) => set({ min: event.target.value })} /></label><label>최댓값<input value={draft.max} onChange={(event) => set({ max: event.target.value })} /></label></div>}</>;
}

function describeRule(rule: RuleSpec): string {
  switch (rule.kind) {
    case "mask_prefix": return `${rule.column} · 앞 ${rule.keep_chars}자 유지`;
    case "not_null": return `${rule.column} · null 금지`;
    case "allowed_values": return `${rule.column} · ${rule.values.join(", ")}`;
    case "range": return `${rule.column} · ${rule.min}–${rule.max}`;
    case "fixed_combination": return rule.columns.join(" + ");
    case "conditional_set": return `${rule.when.column} ${rule.when.operator} ${rule.when.value ?? ""} → ${rule.target}=${rule.value}`;
    case "sum_equals": return `${rule.sources.join(" + ")} = ${rule.target}`;
    case "compare": return `${rule.left} ${rule.op} ${rule.right}`;
  }
}


function legacyExecutiveSummary(report: PrimaryReport): ExecutiveSummary {
  const summary = report.summary ?? {};
  const exact = report.exact && typeof report.exact === "object" && !Array.isArray(report.exact)
    ? report.exact as Record<string, unknown>
    : {};
  const requestedRows = typeof summary.requested_rows === "number" ? summary.requested_rows : null;
  const actualRows = typeof summary.actual_rows === "number" ? summary.actual_rows : null;
  const hardViolations = typeof exact.hard_rule_violations === "number"
    ? exact.hard_rule_violations
    : null;
  const median = typeof summary.median_excess === "number" ? summary.median_excess : null;
  const p95 = typeof summary.p95_excess === "number" ? summary.p95_excess : null;
  const maximum = typeof summary.max_excess === "number" ? summary.max_excess : null;
  const qualityParagraphs: string[] = [];

  if (requestedRows !== null && actualRows !== null) {
    qualityParagraphs.push(
      `요청한 ${requestedRows.toLocaleString("ko-KR")}행 중 `
      + `${actualRows.toLocaleString("ko-KR")}행을 생성했습니다.`
      + (hardViolations === null ? "" : ` 전체 결과의 강제 규칙 위반은 ${hardViolations.toLocaleString("ko-KR")}건입니다.`),
    );
  }
  if (median !== null && p95 !== null && maximum !== null) {
    qualityParagraphs.push(
      `원본 표본 자체의 차이를 뺀 추가 분포 오차는 중앙값 ${median.toFixed(4)}`
      + `(${(median * 100).toFixed(2)}%p), 95백분위 ${p95.toFixed(4)}`
      + `(${(p95 * 100).toFixed(2)}%p), 최댓값 ${maximum.toFixed(4)}`
      + `(${(maximum * 100).toFixed(2)}%p)입니다. 0에 가까울수록 합성자료 때문에 추가된 분포 오차가 작습니다.`,
    );
  }
  const weakestColumns = [...(report.columns ?? [])]
    .filter((column) => typeof column.baseline_excess === "number")
    .sort((left, right) => (right.baseline_excess ?? 0) - (left.baseline_excess ?? 0))
    .slice(0, 3);
  if (weakestColumns.length > 0) {
    qualityParagraphs.push(
      `우선 확인할 열은 ${weakestColumns.map((column) => (
        `${column.name} ${(column.baseline_excess ?? 0).toFixed(4)}`
      )).join(", ")}입니다. 이 열의 분포와 실제 분석 결과를 먼저 확인해야 합니다.`,
    );
  }
  qualityParagraphs.push(
    "목표 열과 분류·회귀 과제가 지정되지 않은 경우 실제 분석 결과를 얼마나 재현하는지는 별도로 확인해야 합니다.",
  );

  const rowConclusion = requestedRows !== null && actualRows !== null
    ? `요청한 ${requestedRows.toLocaleString("ko-KR")}행 중 ${actualRows.toLocaleString("ko-KR")}행을 생성했습니다.`
    : "이전 형식으로 생성된 보고서를 읽기 쉬운 설명으로 변환해 표시했습니다.";
  return {
    overall_conclusion: hardViolations === 0
      ? `${rowConclusion} 강제 규칙 위반 없이 구조 검증을 통과했습니다.`
      : rowConclusion,
    quality: {
      heading: "재현 품질",
      paragraphs: qualityParagraphs,
    },
    privacy: {
      heading: "프라이버시 보호",
      paragraphs: [report.mode === "differential_privacy"
        ? "이 결과에는 차등프라이버시가 적용되었습니다. 정확한 보호 강도는 ε, δ, 보호 단위와 누적 공개 횟수를 함께 확인해야 하며, 이 화면의 내부 품질 지표는 외부 공개용 정보가 아닙니다."
        : "이 일반 합성 결과에는 형식적 차등프라이버시 보장이 없습니다. 분포가 비슷하거나 원본과 가까운 행이 적다는 관측만으로 개인정보가 안전하다고 판단하면 안 됩니다."],
    },
    limitations: [
      "이전 형식의 보고서에는 최신 프라이버시 공격 진단과 분석 목적 재현 결과가 없을 수 있습니다.",
      "품질 지표에는 모든 데이터와 분석 목적에 공통으로 적용할 보편적인 합격 기준이 없습니다.",
    ],
  };
}


function ReportExecutiveSummary({ report }: { report: PrimaryReport }) {
  const summary = report.executive_summary ?? legacyExecutiveSummary(report);
  return (
    <section className="report-executive" aria-labelledby="report-executive-heading">
      <h3 id="report-executive-heading">한눈에 보는 결론</h3>
      <p className="report-overall"><strong>{summary.overall_conclusion}</strong></p>
      {[summary.quality, summary.privacy].map((section) => (
        <div key={section.heading}>
          <h4>{section.heading}</h4>
          {section.paragraphs.map((paragraph) => <p key={paragraph}>{paragraph}</p>)}
        </div>
      ))}
      {summary.limitations.length > 0 && (
        <div>
          <h4>해석할 때 주의할 점</h4>
          <ul>{summary.limitations.map((item) => <li key={item}>{item}</li>)}</ul>
        </div>
      )}
      {report.narrative && report.narrative.length > 0 && (
        <details className="report-narrative">
          <summary>수치까지 포함한 자세한 해설 보기</summary>
          {report.narrative.map((paragraph) => <p key={paragraph}>{paragraph}</p>)}
        </details>
      )}
      <button className="button secondary" type="button" onClick={() => window.print()}>
        이 화면 전체 인쇄·PDF 저장
      </button>
      <p className="report-print-note">
        인쇄하면 세 개 탭(품질 요약 · 열별 거리 · 공개 경계)이 모두 한 문서로 나옵니다.
        문서 파일이 필요하면 아래 <strong>쉬운 품질 보고서(한글)</strong>를 내려받으세요.
      </p>
    </section>
  );
}


function reportRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function finiteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function verdictValues(report: PrimaryReport) {
  const sources = [
    reportRecord(report.summary),
    reportRecord(report.exact),
    reportRecord(report.output),
    report as Record<string, unknown>,
  ];
  const pick = (key: string): number | null => {
    for (const source of sources) {
      const value = finiteNumber(source[key]);
      if (value !== null) return value;
    }
    return null;
  };
  return {
    requested: pick("requested_rows"),
    actual: pick("actual_rows"),
    violations: pick("hard_rule_violations"),
    median: pick("median_excess"),
  };
}

type VerdictTone = "pass" | "warn" | "unknown";

function fidelityBand(median: number | null): { tone: VerdictTone; label: string; hint: string } {
  if (median === null) {
    return { tone: "unknown", label: "확인 불가", hint: "해석 가능한 분포 차이 측정값이 없습니다." };
  }
  if (median < 0.02) return { tone: "pass", label: "차이가 매우 작음", hint: "참고 구간이며 합격 기준은 아닙니다." };
  if (median < 0.05) return { tone: "pass", label: "차이가 작음", hint: "참고 구간이며 합격 기준은 아닙니다." };
  if (median < 0.1) return { tone: "warn", label: "차이가 보통", hint: "사용 목적에 따라 허용 여부가 갈립니다." };
  return { tone: "warn", label: "차이가 큼", hint: "열별 거리 탭에서 원인 열을 먼저 확인하세요." };
}

function ReportVerdict({ report, artifacts }: { report: PrimaryReport; artifacts: ArtifactManifest[] }) {
  const { requested, actual, violations, median } = verdictValues(report);
  const known = requested !== null && actual !== null && violations !== null;
  const structureTone: VerdictTone = !known
    ? "unknown"
    : requested === actual && violations === 0 ? "pass" : "warn";
  const band = fidelityBand(median);
  const plain = artifacts.find((artifact) => artifact.downloadable && artifact.kind.endsWith("_report_hwpx"));
  const headline = structureTone === "pass"
    ? "요청한 행을 모두 만들었고 규칙 위반은 없습니다"
    : structureTone === "warn"
      ? "구조 검증에서 확인할 항목이 있습니다"
      : "구조 검증 결과를 읽을 수 없습니다";
  return (
    <section className="report-verdict" aria-labelledby="report-verdict-heading">
      <div className="report-verdict-head">
        <div>
          <p className="section-label">한눈에 보는 판정</p>
          <h3 id="report-verdict-heading">{headline}</h3>
        </div>
        {plain && (
          <a
            className="button primary"
            href={`/api/v1/artifacts/${plain.artifact_id}/download`}
            download
          >
            쉬운 품질 보고서 받기 (한글 문서)
          </a>
        )}
      </div>
      <dl className="verdict-tiles">
        <div data-tone={structureTone}>
          <dt>생성 행 수</dt>
          <dd>{actual === null ? "확인 불가" : `${actual.toLocaleString("ko-KR")}행`}</dd>
          <p>요청 {requested === null ? "확인 불가" : `${requested.toLocaleString("ko-KR")}행`} · 같아야 정상입니다.</p>
        </div>
        <div data-tone={violations === null ? "unknown" : violations === 0 ? "pass" : "warn"}>
          <dt>강제 규칙 위반</dt>
          <dd>{violations === null ? "확인 불가" : `${violations.toLocaleString("ko-KR")}건`}</dd>
          <p>0건이어야 정상입니다.</p>
        </div>
        <div data-tone={band.tone}>
          <dt>분포 차이 (중앙값)</dt>
          <dd>{median === null ? "확인 불가" : median.toFixed(4)}</dd>
          <p>{band.label} · {band.hint}</p>
        </div>
      </dl>
    </section>
  );
}

const GLOSSARY: Array<[string, string]> = [
  ["재현자료(합성자료)", "원본의 통계적 성질을 흉내 내도록 새로 만든 자료입니다. 특정한 사람의 기록을 그대로 옮긴 것이 아닙니다."],
  ["강제 규칙", "'값이 비면 안 된다', '합계가 맞아야 한다'처럼 반드시 지키도록 지정한 조건입니다. 위반이 0건이어야 정상입니다."],
  ["합성 거리", "원본과 재현자료의 분포가 얼마나 다른지를 0~1로 나타낸 값입니다. 0이면 같습니다."],
  ["기준선 초과 (baseline-excess)", "원본을 둘로 나눠 비교해도 생기는 우연한 차이를 뺀, 재현자료 때문에 추가로 생긴 차이만 남긴 값입니다."],
  ["KS · TVD", "각각 숫자형 열과 범주형 열에서 분포 차이를 재는 방법입니다. 둘 다 0에 가까울수록 잘 재현한 것입니다."],
  ["C2ST · AUROC", "'원본인지 재현자료인지' 맞히는 판별기의 성적입니다. AUROC가 0.5에 가까우면 구별하지 못한다는 뜻입니다."],
  ["TRTR · TSTR", "같은 예측 분석을 원본으로 학습했을 때와 재현자료로 학습했을 때의 성적입니다. 비슷할수록 같은 결론을 얻습니다."],
  ["Gower 최근접거리", "재현자료의 각 행이 원본의 가장 비슷한 행과 얼마나 떨어져 있는지 잰 값입니다. 원본 복제를 살피는 경고 지표입니다."],
  ["Anonymeter", "공격자가 재현자료로 특정인의 비밀 값을 알아맞힐 수 있는지 모의로 시험한 결과입니다."],
  ["차등프라이버시 ε · δ", "한 사람의 기록이 결과에 미치는 영향의 수학적 상한입니다. 안전한 사람의 비율이나 재식별 확률이 아닙니다."],
];

function ReportGlossary() {
  return (
    <details className="report-glossary">
      <summary>이 보고서에 나오는 용어 풀이</summary>
      <dl>
        {GLOSSARY.map(([term, description]) => (
          <div key={term}><dt>{term}</dt><dd>{description}</dd></div>
        ))}
      </dl>
    </details>
  );
}


function ArtifactDownloads({ artifacts, legacyReport }: { artifacts: ArtifactManifest[]; legacyReport: boolean }) {
  const downloadable = artifacts.filter((artifact) => artifact.downloadable);
  const technicalReports = downloadable.filter((artifact) => artifact.kind.endsWith("_report_json"));
  const plainReports = downloadable.filter((artifact) => artifact.kind.endsWith("_report_hwpx"));
  const detailedReports = downloadable.filter((artifact) => artifact.kind.endsWith("_report_html"));
  const data = downloadable.filter((artifact) => !artifact.kind.includes("_report_"));
  return (
    <section className="downloads" aria-labelledby="downloads-heading">
      <div>
        <p className="section-label">결과 파일</p>
        <h3 id="downloads-heading">보고서와 생성 데이터</h3>
      </div>
      <p className="download-boundary-note">
        보고서를 처음 읽는 분은 <strong>쉬운 품질 보고서(한글 문서)</strong>부터 보세요.
        JSON은 시스템 연동과 정밀 검증을 위한 기술 데이터이며 일반적인 검토에는 필요하지 않습니다.
      </p>
      {plainReports.length + detailedReports.length + data.length === 0 ? (
        <p className="empty-note">받을 수 있는 보고서나 생성 데이터가 없습니다.</p>
      ) : (
        <>
          {plainReports.length > 0 && (
            <div className="download-group">
              <h4>쉬운 품질 보고서</h4>
              <p>비전문가도 읽을 수 있도록 결론·핵심 지표·용어 해설만 담은 한글(HWPX) 문서입니다. 한글에서 바로 열립니다.</p>
              <ul>
                {plainReports.map((artifact) => <ArtifactDownloadItem key={artifact.artifact_id} artifact={artifact} legacyReport={legacyReport} />)}
              </ul>
            </div>
          )}
          {detailedReports.length > 0 && (
            <div className="download-group">
              <h4>자세한 품질 보고서</h4>
              <p>모든 측정값과 검증 근거를 함께 담은 웹 문서입니다. 브라우저에서 열어 인쇄하거나 PDF로 저장할 수 있습니다.</p>
              <ul>
                {detailedReports.map((artifact) => <ArtifactDownloadItem key={artifact.artifact_id} artifact={artifact} legacyReport={legacyReport} />)}
              </ul>
            </div>
          )}
          {data.length > 0 && (
            <div className="download-group">
              <h4>생성 데이터</h4>
              <p>이번 작업이 만든 재현자료 파일입니다.</p>
              <ul>
                {data.map((artifact) => <ArtifactDownloadItem key={artifact.artifact_id} artifact={artifact} legacyReport={legacyReport} />)}
              </ul>
            </div>
          )}
        </>
      )}
      {technicalReports.length > 0 && (
        <details className="technical-downloads">
          <summary>시스템 연동용 JSON 데이터</summary>
          <p>자동 처리나 지표 원본 검증이 필요한 경우에만 사용합니다.</p>
          <ul>
            {technicalReports.map((artifact) => <ArtifactDownloadItem key={artifact.artifact_id} artifact={artifact} legacyReport={legacyReport} />)}
          </ul>
        </details>
      )}
    </section>
  );
}


function ArtifactDownloadItem({ artifact, legacyReport }: { artifact: ArtifactManifest; legacyReport: boolean }) {
  const restricted = artifact.contains_private_source_information || !artifact.release_safe;
  const isReadableReport = artifact.kind.endsWith("_report_html");
  const isPlainReport = artifact.kind.endsWith("_report_hwpx");
  return (
    <li>
      <div>
        <strong>{legacyReport && isReadableReport ? "이전 형식 품질 보고서 (기술 상세 포함)" : artifactName(artifact.kind)}</strong>
        <span>{formatBytes(artifact.size_bytes)} · SHA-256 검증됨</span>
        <span className={`artifact-boundary ${restricted ? "restricted" : "release-safe"}`}>
          {artifact.contains_private_source_information
            ? "내부 검토용 · 원본 정보 포함 가능"
            : artifact.release_safe
              ? "공개 경계 통과"
              : "외부 공개 미승인"}
        </span>
      </div>
      <a className={`button ${isPlainReport ? "primary" : "secondary"}`} href={`/api/v1/artifacts/${artifact.artifact_id}/download`} download>
        {isPlainReport ? "한글 문서 받기" : isReadableReport ? legacyReport ? "이전 보고서 받기" : "웹 문서 받기" : "파일 받기"}
      </a>
    </li>
  );
}


function DpReleaseSummary({ report }: { report: PrimaryReport }) {
  const ledger = report.ledger;
  const output = report.output;
  const ledgerValues = ledger && typeof ledger === "object" && !Array.isArray(ledger)
    ? ledger as Record<string, unknown>
    : {};
  const outputValues = output && typeof output === "object" && !Array.isArray(output)
    ? output as Record<string, unknown>
    : {};
  const summaryValues = report.summary ?? {};
  const entries = [
    ["Privacy unit", ledgerValues.privacy_unit],
    ["인접성", ledgerValues.adjacency],
    ["ε model", ledgerValues.epsilon_model],
    ["δ", ledgerValues.delta],
    ["요청 행", outputValues.requested_rows ?? summaryValues.requested_rows],
    ["생성 행", outputValues.actual_rows ?? summaryValues.actual_rows],
  ].filter((entry): entry is [string, string | number] => (
    typeof entry[1] === "string" || typeof entry[1] === "number"
  ));
  return <section aria-labelledby="dp-release-summary-heading">
    <h3 id="dp-release-summary-heading">{report.release_safe === true ? "형식적 DP 공개 요약" : "형식적 DP 보호 요약"}</h3>
    <dl className="metric-list">{entries.map(([label, value]) => <div key={label}><dt>{label}</dt><dd>{typeof value === "number" ? value.toLocaleString("ko-KR") : value}</dd></div>)}</dl>
  </section>;
}

function AdvancedEvaluation({ report }: { report: PrimaryReport }) {
  const advanced = report.advanced;
  if (!advanced || typeof advanced !== "object" || Array.isArray(advanced)) return null;
  const metrics = advanced as Record<string, unknown>;
  const section = (name: string): Record<string, unknown> | null => {
    const value = metrics[name];
    return value && typeof value === "object" && !Array.isArray(value)
      ? value as Record<string, unknown>
      : null;
  };
  const pairwise = section("pairwise");
  const c2st = section("c2st");
  const downstream = section("downstream_utility");
  const privacy = section("empirical_privacy");
  const decimals = (value: unknown) => (typeof value === "number" && Number.isFinite(value) ? value.toFixed(4) : null);
  const pairsConsidered = finiteNumber(pairwise?.pairs_considered);
  const c2stAuroc = decimals(reportRecord(reportRecord(c2st?.synthetic_vs_untouched_holdout).nonlinear).auroc)
    ?? decimals(reportRecord(reportRecord(c2st?.synthetic_vs_untouched_holdout).linear).auroc);
  const trtr = decimals(downstream?.trtr);
  const tstr = decimals(downstream?.tstr);
  const gowerGap = decimals(reportRecord(reportRecord(reportRecord(privacy?.gower).dcr)).synthetic_median_minus_control);
  const entries: Array<[string, string, string]> = [
    [
      "열 쌍 관계",
      pairsConsidered === null ? "적용 불가" : `${pairsConsidered.toLocaleString("ko-KR")}개 쌍 비교`,
      "두 열 사이의 관계까지 재현했는지 봅니다.",
    ],
    [
      "실제/합성 판별 (AUROC)",
      c2stAuroc ?? "적용 불가",
      "0.5에 가까울수록 원본과 구별하기 어렵다는 뜻입니다.",
    ],
    [
      "분석 재현 (TRTR → TSTR)",
      trtr && tstr ? `${trtr} → ${tstr}` : "지정 안 함",
      "두 값이 비슷할수록 같은 분석 결론을 얻습니다.",
    ],
    [
      "원본 근접 진단 (Gower)",
      gowerGap ?? "적용 불가",
      "음수이거나 0에 가까우면 원본 근접 가능성을 더 살펴야 합니다.",
    ],
  ];
  return <section className="advanced-evaluation" aria-labelledby="advanced-evaluation-heading">
    <h3 id="advanced-evaluation-heading">고급 평가</h3>
    <p>단일 종합 점수 대신 열 쌍 관계, 분류 기반 구별 가능성, 명시적 downstream 작업을 분리해 해석합니다. 경험적 진단은 형식적 개인정보 보호 보장이 아닙니다.</p>
    <dl className="metric-list explained">
      {entries.map(([label, value, hint]) => (
        <div key={label}><dt>{label}</dt><dd>{value}</dd><p>{hint}</p></div>
      ))}
    </dl>
  </section>;
}

// The verdict tiles already carry requested/actual rows and the median, so this list
// shows only the spread around it.
const VERDICT_COVERED_METRICS = new Set(["requested_rows", "actual_rows", "median_excess"]);

function ReportSummary({ report }: { report: PrimaryReport }) {
  const entries = Object.entries(report.summary ?? {}).filter(
    ([key, value]) => !VERDICT_COVERED_METRICS.has(key)
      && (typeof value === "number" || typeof value === "string"),
  );
  if (entries.length === 0) return null;
  return (
    <section aria-labelledby="report-spread-heading">
      <h3 id="report-spread-heading">분포 차이의 퍼짐</h3>
      <p>중앙값은 위 판정에 있습니다. 아래 두 값은 성적이 나쁜 쪽 열이 얼마나 떨어졌는지 보여줍니다.</p>
      <dl className="metric-list">
        {entries.map(([key, value]) => (
          <div key={key}>
            <dt>{reportMetricLabel(key)}</dt>
            <dd>{typeof value === "number" ? value.toLocaleString("ko-KR", { maximumFractionDigits: 4 }) : value}</dd>
          </div>
        ))}
      </dl>
    </section>
  );
}

function ReportColumns({ report }: { report: PrimaryReport }) {
  const columns = [...(report.columns ?? [])].sort(
    (left, right) => (right.baseline_excess ?? -1) - (left.baseline_excess ?? -1),
  );
  if (columns.length === 0) return <p className="empty-note">열별 적용 가능 집계가 없습니다.</p>;
  return (
    <>
      <p className="table-lead">
        원본과의 차이가 큰 열부터 정렬했습니다. 위쪽 열일수록 재현이 어려웠다는 뜻이므로,
        그 열을 실제 분석에 쓸 계획이라면 원본 분포와 한 번 더 비교하세요.
      </p>
      <div className="table-wrap" tabIndex={0} role="region" aria-label="열별 거리 표">
        <table className="report-table">
          <thead>
            <tr>
              <th scope="col">열</th><th scope="col">지표</th><th scope="col">합성 거리</th>
              <th scope="col">기준선 초과</th><th scope="col">결측 차이</th>
            </tr>
          </thead>
          <tbody>
            {columns.map((column) => (
              <tr key={column.name} data-attention={(column.baseline_excess ?? 0) >= 0.1 ? "high" : undefined}>
                <th scope="row">{column.name}</th>
                <td>{columnMetricLabel(column.metric)}</td>
                <td>{column.distance?.toFixed(4) ?? "N/A"}</td>
                <td>{column.baseline_excess?.toFixed(4) ?? "N/A"}</td>
                <td>{column.missingness_difference?.toFixed(4) ?? "N/A"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function columnMetricLabel(metric: string | undefined): string {
  if (!metric) return "—";
  const normalized = metric.toUpperCase();
  if (normalized.includes("KS")) return "KS · 분포 차이";
  if (normalized.includes("TVD")) return "TVD · 비율 차이";
  if (normalized.includes("GOWER")) return "Gower · 근접 거리";
  return metric;
}

function reportMetricLabel(key: string): string {
  const labels: Record<string, string> = { requested_rows: "요청 행", actual_rows: "생성 행", median_excess: "기준선 초과 중앙값", p95_excess: "기준선 초과 p95", max_excess: "기준선 초과 최댓값" };
  return labels[key] ?? key.replaceAll("_", " ");
}

function artifactName(kind: string): string {
  const labels: Record<string, string> = { synthetic_parquet_zip: "합성 데이터 묶음 (Parquet ZIP64)", synthetic_parquet_manifest: "합성 데이터 파일 목록", synthetic_csv: "합성 데이터 (CSV)", primary_report_hwpx: "쉬운 품질 보고서 (한글 문서)", dp_release_report_hwpx: "외부 공개용 쉬운 품질 보고서 (한글 문서)", primary_report_html: "자세한 품질 보고서 (웹 문서)", primary_report_json: "내부 품질 지표 원본 (JSON)", dp_release_report_html: "외부 공개용 자세한 DP 보고서 (웹 문서)", dp_release_report_json: "DP 공개 지표 원본 (JSON)" };
  return labels[kind] ?? kind.replaceAll("_", " ");
}
