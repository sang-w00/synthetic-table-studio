import { type KeyboardEvent, useEffect, useMemo, useRef, useState } from "react";

import {
  ApiProblem,
  api,
  type ArtifactManifest,
  type ColumnKind,
  type ColumnProfile,
  type ColumnRole,
  type ColumnSchema,
  type DatasetProfile,
  type JobSnapshot,
  type ParseOptions,
  type PrimaryReport,
  type ProgressEventPayload,
  type RuleKind,
  type RuleSpec,
  type SheetDescriptor,
  type UploadProgress,
  type UtilitySynthesisRequest,
} from "./api";
import { ReportChart } from "./ReportChart";
import { findRuleConflicts } from "./rules";

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

  const [file, setFile] = useState<File | null>(null);
  const [uploadProgress, setUploadProgress] = useState<UploadProgress | null>(null);
  const [datasetId, setDatasetId] = useState<string | null>(null);
  const [datasetManifestSha, setDatasetManifestSha] = useState<string | null>(null);
  const [uploadBranch, setUploadBranch] = useState<UploadBranch>("none");
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

  const [rules, setRules] = useState<RuleSpec[]>([]);
  const [ruleDraft, setRuleDraft] = useState<RuleDraft>(INITIAL_RULE_DRAFT);
  const [ruleDraftError, setRuleDraftError] = useState<string | null>(null);
  const [rulesVersion, setRulesVersion] = useState("0");

  const [outputRows, setOutputRows] = useState(100_000);
  const [outputFormats, setOutputFormats] = useState<Array<"parquet" | "csv">>(["parquet"]);
  const [trainingRows, setTrainingRows] = useState(50_000);
  const [maxEpochs, setMaxEpochs] = useState(5);
  const [maxMinutes, setMaxMinutes] = useState(60);
  const [modelSize, setModelSize] = useState("small");
  const [device, setDevice] = useState("cpu");
  const [resourceProfile, setResourceProfile] = useState("m4_local");
  const [generationSeed, setGenerationSeed] = useState(20260723);

  const [job, setJob] = useState<JobSnapshot | null>(null);
  const [jobProgress, setJobProgress] = useState<ProgressEventPayload | null>(null);
  const [report, setReport] = useState<PrimaryReport | null>(null);
  const [artifacts, setArtifacts] = useState<ArtifactManifest[]>([]);
  const [reportTab, setReportTab] = useState<ReportTab>("summary");
  const eventSource = useRef<EventSource | null>(null);
  const stageFocusRequested = useRef(false);

  const conflicts = useMemo(() => findRuleConflicts(rules), [rules]);
  const currentStageIndex = STAGES.findIndex((item) => item.id === stage);

  useEffect(() => {
    document.documentElement.lang = "ko";
    void api
      .bootstrap()
      .then(() => {
        setSessionReady(true);
        setGlobalStatus("준비되었습니다.");
      })
      .catch((bootstrapError: unknown) => {
        setError(`SESSION_REQUIRED: ${displayError(bootstrapError)}`);
        setGlobalStatus("초기화에 실패했습니다.");
      });
    return () => eventSource.current?.close();
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

  async function startUpload(): Promise<void> {
    if (!file) {
      setError("업로드할 CSV 또는 XLSX 파일을 선택하세요.");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      const snapshot = await api.uploadFile(file, setUploadProgress);
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
      const [reportPayload, artifactPayload] = await Promise.all([
        api.getPrimaryReport(jobId),
        api.getArtifacts(jobId),
      ]);
      const evaluation = reportPayload.evaluation ?? reportPayload;
      setReport({
        ...evaluation,
        narrative: reportPayload.narrative ?? evaluation.narrative,
      });
      setArtifacts(artifactPayload.artifacts);
      setGlobalStatus("합성 데이터와 품질 보고서가 준비되었습니다.");
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

  async function createJob(): Promise<void> {
    if (!datasetId || !datasetManifestSha || outputFormats.length === 0) {
      setError("정규화된 데이터셋과 한 개 이상의 출력 형식이 필요합니다.");
      return;
    }
    const request: UtilitySynthesisRequest = {
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
    };
    setBusy(true);
    setError(null);
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
              <div className="table-wrap">
                <table className="schema-table">
                  <caption className="visually-hidden">열 이름, 프로파일, 유형, 역할, 결측 허용 편집</caption>
                  <thead><tr><th scope="col">열 / 관찰값</th><th scope="col">유형</th><th scope="col">역할</th><th scope="col">결측</th></tr></thead>
                  <tbody>{columns.map((column, index) => {
                    const observed = profile.columns[index] as ColumnProfile | undefined;
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
              <div className="stage-heading-row"><div><p className="section-label">04 · Method</p><h2 id="stage-heading" tabIndex={-1}>생성 모드와 자원</h2></div><p className="stage-note">M4 로컬 기본 lease</p></div>
              <div className="mode-grid" role="radiogroup" aria-label="합성 모드">
                <label className="mode-card selected">
                  <input type="radio" name="mode" value="utility" checked readOnly />
                  <span className="mode-tag">사용 가능</span>
                  <strong>일반 고품질 합성</strong>
                  <span>TabularARGN · 관계와 조건부 분포 중심</span>
                  <em>
                    원본의 분포와 열 관계를 학습하지만, ε/δ로 보정된 노이즈를 쓰지 않습니다.
                    희귀값이나 특이한 조합이 재현될 수 있으므로 멤버십·속성 추론에 대한 수학적
                    보호 보장은 없습니다.
                  </em>
                </label>
                <label className="mode-card disabled" aria-describedby="dp-audit dp-boundary">
                  <input type="radio" name="mode" value="differential_privacy" disabled />
                  <span className="mode-tag">계약 검증 통과 · 실행 경로 준비 중</span>
                  <strong>형식적 차등프라이버시</strong>
                  <span>MST · 행 단위 add/remove 인접성</span>
                  <em id="dp-audit">
                    고정된 dpmm 0.1.9 checkpoint는 trusted curator 내부에만 보관하고 공개하지
                    않습니다. 새 생성 프로세스는 공개 sampling seed로 학습 RNG를 교체한다는
                    계약을 통과했습니다. 현재는 앱의 DP 실행 경로가 아직 비활성화되어 선택할 수
                    없습니다.
                  </em>
                </label>
              </div>
              <div id="dp-boundary" className="boundary-note">
                <strong>DP 공개 경계란?</strong>
                <p>
                  원본을 볼 수 있는 내부 작업 영역과 외부로 내보낼 수 있는 산출물 사이의 선입니다.
                  형식적 DP에서는 이 선 밖으로 DP 메커니즘의 결과와 사전에 공개한 메타데이터만
                  나가야 합니다.
                </p>
                <dl>
                  <div><dt>경계 안</dt><dd>원본, 비공개 프로파일, 학습 체크포인트, 내부 진단 보고서</dd></div>
                  <div><dt>경계 밖</dt><dd>release_safe=true이고 원본 정보를 포함하지 않는 결과와 공개 보고서만 허용</dd></div>
                  <div><dt>지원 범위</dt><dd>공개 범위·범주가 정해진 모델링 열 최대 32개와 공개 수식 파생 열</dd></div>
                  <div><dt>현재 상태</dt><dd>trusted curator checkpoint와 공개 sampling seed 계약은 통과했지만 앱 실행 경로는 아직 비활성화됨</dd></div>
                </dl>
              </div>
              <aside className="guidance-panel" aria-labelledby="training-guidance-heading">
                <h3 id="training-guidance-heading">권장 학습 설정</h3>
                <ul>
                  <li><strong>1 epoch</strong><span>업로드·규칙·다운로드가 동작하는지 빠르게 확인할 때</span></li>
                  <li><strong>5 epoch</strong><span>현재 sample gate를 통과한 기본 권장값. 첫 품질 비교에 사용</span></li>
                  <li><strong>10 epoch 이상</strong><span>시간이 더 들며 자동으로 더 좋아지지는 않음. 보고서 지표를 비교해 결정</span></li>
                  <li><strong>Small 모델</strong><span>현재 adapter와 M4에서 검증된 유일한 크기. Medium/Large는 검증 전까지 선택 불가</span></li>
                </ul>
              </aside>
              <fieldset className="resource-form">
                <legend>일반 합성 실행 설정</legend>
                <div className="form-grid three">
                  <label>출력 행 수<input type="number" min={1} max={55_000_000} value={outputRows} onChange={(event) => setOutputRows(Number(event.target.value))} /></label>
                  <label>학습 표본 상한<input type="number" min={1} max={250_000} value={trainingRows} onChange={(event) => setTrainingRows(Number(event.target.value))} /><small>이 호스트 기본 상한 250,000행</small></label>
                  <label>자원 프로필<select value={resourceProfile} onChange={(event) => setResourceProfile(event.target.value)}><option value="m4_local">M4 로컬 · 24 GiB worker</option><option value="m4_conservative">M4 보수적 · CPU</option></select></label>
                  <label>최대 epoch<input type="number" min={1} max={100} value={maxEpochs} onChange={(event) => setMaxEpochs(Number(event.target.value))} /><small>첫 실행 권장: 5</small></label>
                  <label>최대 학습 시간 (분)<input type="number" min={1} max={1440} value={maxMinutes} onChange={(event) => setMaxMinutes(Number(event.target.value))} /></label>
                  <label>모델 크기<select value={modelSize} onChange={(event) => setModelSize(event.target.value)}><option value="small">Small · 검증됨</option><option value="medium" disabled>Medium · 미검증</option><option value="large" disabled>Large · 미검증</option></select></label>
                  <label>실행 장치<select value={device} onChange={(event) => setDevice(event.target.value)}><option value="cpu">CPU (검증됨)</option><option value="mps" disabled>MPS (parity gate 필요)</option></select></label>
                  <label>생성 seed<input type="number" min={0} max={4_294_967_295} value={generationSeed} onChange={(event) => setGenerationSeed(Number(event.target.value))} /></label>
                </div>
                <fieldset className="format-fieldset">
                  <legend>출력 형식</legend>
                  <label className="checkbox-row"><input type="checkbox" checked={outputFormats.includes("parquet")} onChange={(event) => setOutputFormats((current) => event.target.checked ? [...new Set([...current, "parquet" as const])] : current.filter((value) => value !== "parquet"))} />Parquet shards + ZIP64 manifest</label>
                  <label className="checkbox-row"><input type="checkbox" checked={outputFormats.includes("csv")} onChange={(event) => setOutputFormats((current) => event.target.checked ? [...new Set([...current, "csv" as const])] : current.filter((value) => value !== "csv"))} />CSV</label>
                </fieldset>
              </fieldset>
              <div className="action-row"><p>시작 전 디스크·RAM admission을 통과하지 못하면 산출물을 만들지 않습니다.</p><button className="button primary" type="button" disabled={busy || outputFormats.length === 0} onClick={() => void createJob()}>일반 합성 시작</button></div>
            </>
          )}

          {stage === "progress" && job && (
            <>
              <div className="stage-heading-row"><div><p className="section-label">05 · Execution</p><h2 id="stage-heading" tabIndex={-1}>합성 작업 진행</h2></div><p className="stage-note">시도 {job.attempt ?? 1}</p></div>
              <div className="job-overview"><div><span className="status-symbol" aria-hidden="true">{job.state === "cancelled" || job.state === "failed" ? "!" : "↻"}</span><div><strong>{JOB_STAGE_LABELS[jobProgress?.stage ?? job.state] ?? job.state}</strong><span>작업 ID {job.job_id}</span></div></div><strong>{jobPercent}%</strong></div>
              <progress className="job-progress" max={100} value={jobPercent} aria-label="합성 작업 진행률">{jobPercent}%</progress>
              <ol className="pipeline-list" aria-label="서버 처리 단계">{["preparing", "fitting", "generating", "repairing", "evaluating", "exporting", "publishing"].map((pipelineStage) => { const known = Object.keys(JOB_STAGE_LABELS).indexOf(jobProgress?.stage ?? job.state); const index = Object.keys(JOB_STAGE_LABELS).indexOf(pipelineStage); return <li key={pipelineStage} data-state={pipelineStage === (jobProgress?.stage ?? job.state) ? "current" : index < known ? "complete" : "upcoming"}><span aria-hidden="true">{index < known ? "✓" : pipelineStage === (jobProgress?.stage ?? job.state) ? "→" : "·"}</span>{JOB_STAGE_LABELS[pipelineStage]}</li>; })}</ol>
              <div className="action-row"><p>페이지를 다시 열어도 retained SSE 이벤트를 마지막 ID부터 재생할 수 있습니다.</p>{job.legal_actions?.includes("resume") || job.state === "cancelled" ? <button className="button primary" type="button" disabled={busy} onClick={() => void resumeJob()}>새 작업으로 재개</button> : <button className="button danger" type="button" disabled={busy || ["succeeded", "failed"].includes(job.state)} onClick={() => void cancelJob()}>작업 취소</button>}</div>
            </>
          )}

          {stage === "report" && report && job && (
            <>
              <div className="stage-heading-row"><div><p className="section-label">06 · Evidence</p><h2 id="stage-heading" tabIndex={-1}>품질 보고서와 산출물</h2></div><p className="stage-note">완료 · utility</p></div>
              <div className="report-callout"><span aria-hidden="true">!</span><p><strong>개인정보 보호 보장 없음</strong>이 일반 합성 결과와 품질 보고서는 형식적 DP 공개 산출물이 아닙니다. 다운로드 전에 조직의 공개 기준을 별도로 검토하세요.</p></div>
              <div className="report-tabs">
                <div role="tablist" aria-label="보고서 보기">{(["summary", "columns", "boundary"] as ReportTab[]).map((tab) => <button key={tab} type="button" role="tab" aria-selected={reportTab === tab} aria-controls={`report-${tab}`} id={`tab-${tab}`} tabIndex={reportTab === tab ? 0 : -1} onClick={() => setReportTab(tab)} onKeyDown={(event) => navigateReportTabs(event, tab)}>{tab === "summary" ? "품질 요약" : tab === "columns" ? "열별 거리" : "공개 경계"}</button>)}</div>
                {reportTab === "summary" && <div role="tabpanel" id="report-summary" aria-labelledby="tab-summary"><ReportNarrative report={report} /><ReportSummary report={report} /><ReportChart report={report} /></div>}
                {reportTab === "columns" && <div role="tabpanel" id="report-columns" aria-labelledby="tab-columns"><ReportColumns report={report} /></div>}
                {reportTab === "boundary" && <div role="tabpanel" id="report-boundary" aria-labelledby="tab-boundary"><div className="boundary-note utility"><strong>Utility / curator 경계</strong><p>원본 프로파일, holdout 품질, 공격 진단과 이 보고서는 private source information을 포함할 수 있으며 release_safe=false입니다. 경험적 공격 지표는 개인정보 보호 보증이 아닙니다.</p></div></div>}
              </div>
              <section className="downloads" aria-labelledby="downloads-heading"><div><p className="section-label">Published artifacts</p><h3 id="downloads-heading">다운로드</h3></div>{artifacts.length === 0 ? <p className="empty-note">게시된 다운로드 산출물이 없습니다.</p> : <ul>{artifacts.filter((artifact) => artifact.downloadable).map((artifact) => <li key={artifact.artifact_id}><div><strong>{artifactName(artifact.kind)}</strong><span>{formatBytes(artifact.size_bytes)} · SHA-256 검증됨</span></div><a className="button secondary" href={`/api/v1/artifacts/${artifact.artifact_id}/download`}>파일 받기</a></li>)}</ul>}</section>
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

function reportNumber(report: PrimaryReport, key: string): number | null {
  const value = report.summary?.[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function metricText(value: number | null, digits = 4): string {
  return value === null
    ? "계산되지 않음"
    : value.toLocaleString("ko-KR", { maximumFractionDigits: digits });
}

function ReportNarrative({ report }: { report: PrimaryReport }) {
  const requestedRows = reportNumber(report, "requested_rows");
  const actualRows = reportNumber(report, "actual_rows");
  const medianExcess = reportNumber(report, "median_excess");
  const p95Excess = reportNumber(report, "p95_excess");
  const maxExcess = reportNumber(report, "max_excess");
  const columns = report.columns ?? [];
  const missingness = columns
    .filter((column) => typeof column.missingness_difference === "number")
    .sort((left, right) => (right.missingness_difference ?? 0) - (left.missingness_difference ?? 0))[0];
  return (
    <section className="report-narrative" aria-labelledby="report-narrative-heading">
      <h3 id="report-narrative-heading">보고서 해설</h3>
      <p>
        {metricText(requestedRows, 0)}행을 요청했고 실제 {metricText(actualRows, 0)}행이
        생성되었습니다. 아래 품질 지표는 원본과 합성 데이터의 분포 유사도를 측정하며,
        개인정보 보호 수준을 측정하지는 않습니다.
      </p>
      <p>
        열별 기준선 초과 거리는 중앙값 {metricText(medianExcess)}, p95 {metricText(p95Excess)},
        최댓값 {metricText(maxExcess)}입니다. 이 값은 원본 내부 표본 간 차이보다 합성 데이터가
        얼마나 더 멀어진지를 나타내며 0에 가까울수록 좋습니다.
      </p>
      <p>
        비교 가능한 열은 {columns.length.toLocaleString("ko-KR")}개입니다.
        {missingness
          ? ` 결측률 차이가 가장 큰 열은 ${missingness.name} (${metricText(missingness.missingness_difference ?? null)})입니다.`
          : " 계산 가능한 결측률 차이는 없습니다."}
      </p>
      <p>
        <code>_RARE_</code>는 ARGN이 학습 빈도가 낮은 범주를 하나로 묶을 때 사용하는 표식입니다.
        식별자로 확인한 열은 모델 입력에서 제외하고 고유한 순번으로 다시 만듭니다. 일반 범주에서
        이 표식을 허용하지 않으려면 허용값 규칙으로 출력 도메인을 명시하세요.
      </p>
    </section>
  );
}


function ReportSummary({ report }: { report: PrimaryReport }) {
  const entries = Object.entries(report.summary ?? {}).filter(([, value]) => typeof value === "number" || typeof value === "string");
  if (entries.length === 0) return <p className="empty-note">보고서 요약 집계가 없습니다.</p>;
  return <dl className="metric-list">{entries.map(([key, value]) => <div key={key}><dt>{reportMetricLabel(key)}</dt><dd>{typeof value === "number" ? value.toLocaleString("ko-KR", { maximumFractionDigits: 4 }) : value}</dd></div>)}</dl>;
}

function ReportColumns({ report }: { report: PrimaryReport }) {
  const columns = report.columns ?? [];
  if (columns.length === 0) return <p className="empty-note">열별 적용 가능 집계가 없습니다.</p>;
  return <div className="table-wrap"><table className="report-table"><thead><tr><th scope="col">열</th><th scope="col">지표</th><th scope="col">합성 거리</th><th scope="col">기준선 초과</th><th scope="col">결측 차이</th></tr></thead><tbody>{columns.map((column) => <tr key={column.name}><th scope="row">{column.name}</th><td>{column.metric ?? "—"}</td><td>{column.distance?.toFixed(4) ?? "N/A"}</td><td>{column.baseline_excess?.toFixed(4) ?? "N/A"}</td><td>{column.missingness_difference?.toFixed(4) ?? "N/A"}</td></tr>)}</tbody></table></div>;
}

function reportMetricLabel(key: string): string {
  const labels: Record<string, string> = { requested_rows: "요청 행", actual_rows: "생성 행", median_excess: "기준선 초과 중앙값", p95_excess: "기준선 초과 p95", max_excess: "기준선 초과 최댓값" };
  return labels[key] ?? key.replaceAll("_", " ");
}

function artifactName(kind: string): string {
  const labels: Record<string, string> = { synthetic_parquet_zip: "합성 Parquet ZIP64", synthetic_parquet_manifest: "Parquet shard manifest", synthetic_csv: "합성 CSV", primary_report_html: "품질 보고서 HTML", primary_report_json: "품질 보고서 JSON" };
  return labels[kind] ?? kind.replaceAll("_", " ");
}
