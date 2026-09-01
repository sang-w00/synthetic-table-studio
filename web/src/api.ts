export const MAX_UPLOAD_CHUNK_BYTES = 64 * 1024 * 1024;

export type DatasetState =
  | "uploading"
  | "parse_options_required"
  | "sheet_required"
  | "raw_ready"
  | "profiled"
  | "schema_ready"
  | "normalized"
  | "failed"
  | string;

export interface DatasetSnapshot {
  dataset_id: string;
  state: DatasetState;
  attempt?: number;
  manifest_sha256?: string;
  legal_actions?: string[];
  progress?: ProgressEventPayload | null;
}
export interface RecoverableDataset extends DatasetSnapshot {
  filename: string;
  size_bytes: number;
  source_format: "csv" | "xlsx";
  upload_offset: number;
  created_at: string;
  updated_at: string;
}

export interface DatasetList {
  version: "1.0";
  datasets: RecoverableDataset[];
}

export interface PersistedSchema {
  version: "1.0";
  dataset_id: string;
  schema_version: string;
  columns: ColumnSchema[];
}

export interface PersistedRules {
  version: "1.0";
  dataset_id: string;
  rules_version: string;
  rules: RuleSpec[];
}


export interface UploadSession extends DatasetSnapshot {
  upload_id: string;
  upload_offset: number;
}

export interface ParseOptions {
  encoding: "utf-8" | "utf-8-sig" | "cp949" | "euc-kr";
  delimiter: string;
  quotechar: string | null;
  escapechar: string | null;
  has_header: boolean;
  malformed: "fail" | "skip";
}

export interface CsvParseCandidate {
  encoding: ParseOptions["encoding"];
  delimiter: string;
  detected_columns?: number;
  sampled_records?: number;
  consistent_records?: number;
}

export interface CsvParseProposal extends Partial<ParseOptions> {
  sample_size_bytes?: number;
  candidates?: CsvParseCandidate[];
  recommended?: CsvParseCandidate;
  ambiguous?: boolean;
}

export interface ParseOptionsResponse extends DatasetSnapshot {
  proposal: CsvParseProposal | null;
  confirmation: ParseOptions | null;
  malformed_preview: Array<Record<string, unknown> | string>;
}

export interface SheetDescriptor {
  name: string;
  rows?: number;
  columns?: number;
  hidden?: boolean;
}

export interface SheetsResponse extends DatasetSnapshot {
  sheets: SheetDescriptor[];
  requires_sheet_selection: boolean;
  selected_sheet: string | null;
}

export type ColumnKind =
  | "integer"
  | "fixed_decimal"
  | "float"
  | "categorical"
  | "boolean"
  | "date"
  | "datetime"
  | "text"
  | "identifier"
  | "excluded";

export type ColumnRole = "model" | "derived" | "identifier" | "excluded";

export interface ColumnProfile {
  name: string;
  storage_type: string;
  row_count: number;
  null_count: number;
  nonnull_count: number;
  minimum: string | null;
  maximum: string | null;
  approx_cardinality: number;
  exact_low_cardinality?: Array<{ value: string; count: number }> | null;
  candidate_type: ColumnKind;
  candidate_requires_confirmation?: boolean;
  candidate_alternatives?: ColumnKind[];
}

export interface DatasetProfile {
  version: "1.0";
  view: "raw" | "typed";
  row_count: number;
  column_count: number;
  columns: ColumnProfile[];
  metadata?: Record<string, unknown>;
}

export interface ColumnSchema {
  name: string;
  kind: ColumnKind;
  nullable: boolean;
  role: ColumnRole;
  decimal_places?: number;
  timezone?: string;
  format?: string;
  public_min?: number | string;
  public_max?: number | string;
  public_bins?: Array<number | string>;
  public_categories?: Array<string | number | boolean>;
  identifier_strategy?: "sequential" | "uuid4";
}

export type RuleKind =
  | "mask_prefix"
  | "not_null"
  | "allowed_values"
  | "range"
  | "fixed_combination"
  | "conditional_set"
  | "sum_equals"
  | "compare";

interface RuleBase {
  id: string;
  kind: RuleKind;
  provenance: "public" | "private_inferred";
  source_action: "block" | "drop_row";
}

export type RuleSpec =
  | (RuleBase & { kind: "mask_prefix"; column: string; keep_chars: number })
  | (RuleBase & { kind: "not_null"; column: string })
  | (RuleBase & { kind: "allowed_values"; column: string; values: string[] })
  | (RuleBase & {
      kind: "range";
      column: string;
      min: string;
      max: string;
      inclusive_min: boolean;
      inclusive_max: boolean;
    })
  | (RuleBase & {
      kind: "fixed_combination";
      columns: string[];
      allowed_tuples?: string[][];
    })
  | (RuleBase & {
      kind: "conditional_set";
      when: { column: string; operator: string; value?: string };
      target: string;
      value: string;
    })
  | (RuleBase & {
      kind: "sum_equals";
      sources: string[];
      target: string;
      tolerance: string;
    })
  | (RuleBase & {
      kind: "compare";
      left: string;
      op: "<" | "<=" | ">" | ">=";
      right: string;
      granularity?: string;
    });

export interface ProgressEventPayload {
  version?: string;
  stage: string;
  state: string;
  completed: number;
  total: number;
  unit?: string;
  message_code?: string;
  metrics?: Record<string, unknown>;
}

export interface JobSnapshot {
  job_id: string;
  dataset_id: string;
  state: string;
  attempt?: number;
  retry_of?: string | null;
  resume_boundary?: string | null;
  progress?: ProgressEventPayload | null;
  legal_actions?: string[];
}
export interface RecoverableJob extends JobSnapshot {
  mode: "utility" | "differential_privacy";
  synthesizer: string;
  output_rows: number;
  created_at: string;
  updated_at: string;
}

export interface JobList {
  version: "1.0";
  jobs: RecoverableJob[];
}


export interface UtilitySynthesisRequest {
  version: "1.0";
  dataset_id: string;
  dataset_manifest_sha: string;
  schema_version: string;
  rules_version: string;
  mode: "utility";
  synthesizer: "tabular_argn";
  output_rows: number;
  output_formats: Array<"parquet" | "csv">;
  resource_profile: string;
  evaluation_config_version: "1.0";
  generation_seed?: number;
  training: {
    max_rows: number;
    max_epochs: number;
    max_minutes: number;
    model_size: string;
    device: string;
  };
}

export interface ManifestFile {
  relative_path: string;
  sha256: string;
  size_bytes: number;
}

export interface DifferentialPrivacySynthesisRequest {
  version: "1.0";
  dataset_id: string;
  dataset_manifest_sha: string;
  schema_version: string;
  rules_version: string;
  mode: "differential_privacy";
  synthesizer: "mst";
  output_rows: number;
  output_formats: Array<"parquet" | "csv">;
  resource_profile: string;
  evaluation_config_version: "1.0";
  privacy: {
    adjacency: "add_remove_one_row";
    privacy_unit: "row";
    epsilon_model: string;
    delta: string;
    epsilon_preprocess: 0;
    public_metadata_manifest: ManifestFile;
    public_target_count: number;
    fit_sampling_rate: string;
    sampling_seed: number;
  };
}

export type SynthesisRequest = UtilitySynthesisRequest | DifferentialPrivacySynthesisRequest;

export interface ArtifactManifest {
  artifact_id: string;
  kind: string;
  size_bytes: number;
  downloadable: boolean;
  release_safe: boolean;
  contains_private_source_information: boolean;
  metadata?: Record<string, unknown>;
}

export interface ArtifactList {
  job_id: string;
  scope: "downloadable" | "dp_release" | "internal";
  artifacts: ArtifactManifest[];
}

export interface HostResources {
  platform_system: string;
  platform_machine: string;
  logical_cpu_count: number;
  total_memory_bytes: number;
  available_memory_bytes: number;
  disk_total_bytes: number;
  disk_free_bytes: number;
  gpu_backend: "none" | "mps" | "cuda";
  gpu_device_count: number;
  gpu_name: string | null;
  gpu_memory_total_bytes: number | null;
}

export interface ResourcePlan {
  resource_profile: string;
  recommended_device: string;
  worker_lease_bytes: number;
  utility_max_rows: number;
  duckdb_memory_limit_bytes: number;
  max_concurrent_jobs: number;
  disk_free_bytes: number;
}

export interface BootstrapResponse {
  status: "ready";
  host_resources: HostResources;
  resource_plan: ResourcePlan;
}

export interface ReportSection {
  heading: string;
  paragraphs: string[];
}

export interface ExecutiveSummary {
  overall_conclusion: string;
  quality: ReportSection;
  privacy: ReportSection;
  limitations: string[];
}

export interface PrimaryReport {
  version?: string;
  summary?: Record<string, string | number | null>;
  narrative?: string[];
  executive_summary?: ExecutiveSummary;
  evaluation?: PrimaryReport;
  columns?: Array<{
    name: string;
    metric?: string;
    distance?: number | null;
    baseline_excess?: number | null;
    missingness_difference?: number | null;
  }>;
  [key: string]: unknown;
}

export class ApiProblem extends Error {
  readonly status: number;
  readonly code: string;
  readonly context: Record<string, unknown>;

  constructor(status: number, code: string, detail: string, context: Record<string, unknown> = {}) {
    super(detail);
    this.name = "ApiProblem";
    this.status = status;
    this.code = code;
    this.context = context;
  }
}

async function problemFrom(response: Response): Promise<ApiProblem> {
  let payload: Record<string, unknown> = {};
  try {
    payload = (await response.json()) as Record<string, unknown>;
  } catch {
    // A status and stable fallback code are still more useful than a JSON parse error.
  }
  const code = typeof payload.code === "string" ? payload.code : `HTTP_${response.status}`;
  const detail =
    typeof payload.detail === "string"
      ? payload.detail
      : typeof payload.title === "string"
        ? payload.title
        : "요청을 처리하지 못했습니다.";
  const context =
    payload.context && typeof payload.context === "object"
      ? (payload.context as Record<string, unknown>)
      : {};
  return new ApiProblem(response.status, code, detail, context);
}

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  if (init?.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  const response = await fetch(path, { ...init, headers, credentials: "same-origin" });
  if (!response.ok) {
    throw await problemFrom(response);
  }
  return (await response.json()) as T;
}

class Sha256 {
  private readonly state = new Uint32Array([
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
    0x5be0cd19,
  ]);
  private readonly block = new Uint8Array(64);
  private blockLength = 0;
  private bytesHashed = 0;

  update(input: Uint8Array): void {
    let offset = 0;
    this.bytesHashed += input.length;
    while (offset < input.length) {
      const take = Math.min(input.length - offset, this.block.length - this.blockLength);
      this.block.set(input.subarray(offset, offset + take), this.blockLength);
      this.blockLength += take;
      offset += take;
      if (this.blockLength === this.block.length) {
        this.compress(this.block);
        this.blockLength = 0;
      }
    }
  }

  digestHex(): string {
    const bitLengthHigh = Math.floor(this.bytesHashed / 0x20000000);
    const bitLengthLow = (this.bytesHashed << 3) >>> 0;
    this.block[this.blockLength] = 0x80;
    this.blockLength += 1;
    if (this.blockLength > 56) {
      this.block.fill(0, this.blockLength);
      this.compress(this.block);
      this.blockLength = 0;
    }
    this.block.fill(0, this.blockLength, 56);
    const view = new DataView(this.block.buffer);
    view.setUint32(56, bitLengthHigh, false);
    view.setUint32(60, bitLengthLow, false);
    this.compress(this.block);
    return Array.from(this.state, (value) => value.toString(16).padStart(8, "0")).join("");
  }

  private compress(block: Uint8Array): void {
    const constants = SHA256_CONSTANTS;
    const words = new Uint32Array(64);
    const view = new DataView(block.buffer, block.byteOffset, block.byteLength);
    for (let index = 0; index < 16; index += 1) {
      words[index] = view.getUint32(index * 4, false);
    }
    for (let index = 16; index < 64; index += 1) {
      const previous = words[index - 15];
      const beforePrevious = words[index - 2];
      const small0 = rotateRight(previous, 7) ^ rotateRight(previous, 18) ^ (previous >>> 3);
      const small1 =
        rotateRight(beforePrevious, 17) ^ rotateRight(beforePrevious, 19) ^ (beforePrevious >>> 10);
      words[index] = (words[index - 16] + small0 + words[index - 7] + small1) >>> 0;
    }
    let [a, b, c, d, e, f, g, h] = this.state;
    for (let index = 0; index < 64; index += 1) {
      const large1 = rotateRight(e, 6) ^ rotateRight(e, 11) ^ rotateRight(e, 25);
      const choose = (e & f) ^ (~e & g);
      const first = (h + large1 + choose + constants[index] + words[index]) >>> 0;
      const large0 = rotateRight(a, 2) ^ rotateRight(a, 13) ^ rotateRight(a, 22);
      const majority = (a & b) ^ (a & c) ^ (b & c);
      const second = (large0 + majority) >>> 0;
      h = g;
      g = f;
      f = e;
      e = (d + first) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (first + second) >>> 0;
    }
    const values = [a, b, c, d, e, f, g, h];
    for (let index = 0; index < this.state.length; index += 1) {
      this.state[index] = (this.state[index] + values[index]) >>> 0;
    }
  }
}

const SHA256_CONSTANTS = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
  0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
  0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
  0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
  0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
  0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
  0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
  0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
  0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
  0xc67178f2,
]);

function rotateRight(value: number, count: number): number {
  return (value >>> count) | (value << (32 - count));
}

async function sha256File(file: File): Promise<string> {
  const hash = new Sha256();
  const reader = file.stream().getReader();
  for (;;) {
    const result = await reader.read();
    if (result.done) break;
    hash.update(result.value);
  }
  return hash.digestHex();
}

function uploadOffset(response: Response, fallback: number): number {
  const value = response.headers.get("Upload-Offset");
  if (value === null) return fallback;
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new ApiProblem(500, "UPLOAD_OFFSET_INVALID", "서버가 잘못된 업로드 위치를 반환했습니다.");
  }
  return parsed;
}

async function recoverOffset(datasetId: string): Promise<number> {
  const response = await fetch(`/api/v1/datasets/${datasetId}/content`, {
    method: "HEAD",
    credentials: "same-origin",
  });
  if (!response.ok) throw await problemFrom(response);
  return uploadOffset(response, 0);
}

export interface UploadProgress {
  sent: number;
  total: number;
  phase: "hashing" | "uploading" | "inspecting";
}

export const api = {
  async bootstrap(): Promise<BootstrapResponse> {
    const response = await fetch("/api/v1/bootstrap", { credentials: "same-origin" });
    if (!response.ok) throw await problemFrom(response);
    return response.json() as Promise<BootstrapResponse>;
  },

  async uploadFile(
    file: File,
    onProgress: (progress: UploadProgress) => void,
    existing?: RecoverableDataset,
  ): Promise<DatasetSnapshot> {
    const extension = file.name.split(".").pop()?.toLowerCase();
    if (extension !== "csv" && extension !== "xlsx") {
      throw new ApiProblem(422, "INPUT_FORMAT_UNSUPPORTED", "CSV 또는 XLSX 파일만 업로드할 수 있습니다.");
    }
    onProgress({ sent: 0, total: file.size, phase: "hashing" });
    const digest = await sha256File(file);
    const session = existing ?? await requestJson<UploadSession>("/api/v1/datasets/uploads", {
      method: "POST",
      body: JSON.stringify({
        filename: file.name,
        size_bytes: file.size,
        source_format: extension,
      }),
    });
    let offset = await recoverOffset(session.dataset_id);
    if (offset > file.size) {
      throw new ApiProblem(409, "UPLOAD_OFFSET_INVALID", "서버 업로드 위치가 파일 크기를 초과합니다.");
    }
    onProgress({ sent: offset, total: file.size, phase: "uploading" });
    let recoveries = 0;
    while (offset < file.size) {
      const nextOffset = Math.min(offset + MAX_UPLOAD_CHUNK_BYTES, file.size);
      try {
        const response = await fetch(`/api/v1/datasets/${session.dataset_id}/content`, {
          method: "PATCH",
          credentials: "same-origin",
          headers: {
            "Content-Type": "application/offset+octet-stream",
            "Upload-Offset": String(offset),
          },
          body: file.slice(offset, nextOffset),
        });
        if (!response.ok) throw await problemFrom(response);
        const acknowledged = uploadOffset(response, nextOffset);
        if (acknowledged <= offset || acknowledged > file.size) {
          throw new ApiProblem(409, "UPLOAD_OFFSET_INVALID", "업로드 위치가 앞으로 이동하지 않았습니다.");
        }
        offset = acknowledged;
        recoveries = 0;
      } catch (error) {
        if (recoveries >= 3) throw error;
        recoveries += 1;
        const recovered = await recoverOffset(session.dataset_id);
        if (recovered < 0 || recovered > file.size) throw error;
        offset = recovered;
      }
      onProgress({ sent: offset, total: file.size, phase: "uploading" });
    }
    onProgress({ sent: file.size, total: file.size, phase: "inspecting" });
    return requestJson<DatasetSnapshot>(`/api/v1/datasets/${session.dataset_id}/complete`, {
      method: "POST",
      body: JSON.stringify({ sha256: digest }),
    });
  },

  listDatasets: (limit = 20) =>
    requestJson<DatasetList>(`/api/v1/datasets?limit=${limit}`),
  getSchema: (datasetId: string) =>
    requestJson<PersistedSchema>(`/api/v1/datasets/${datasetId}/schema`),
  getRules: (datasetId: string) =>
    requestJson<PersistedRules>(`/api/v1/datasets/${datasetId}/rules`),
  getDataset: (datasetId: string) =>
    requestJson<DatasetSnapshot>(`/api/v1/datasets/${datasetId}`),
  getParseOptions: (datasetId: string) =>
    requestJson<ParseOptionsResponse>(`/api/v1/datasets/${datasetId}/parse-options`),
  confirmParseOptions: (datasetId: string, options: ParseOptions) =>
    requestJson<DatasetSnapshot>(`/api/v1/datasets/${datasetId}/parse-options`, {
      method: "PUT",
      body: JSON.stringify(options),
    }),
  getSheets: (datasetId: string) =>
    requestJson<SheetsResponse>(`/api/v1/datasets/${datasetId}/sheets`),
  selectSheet: (datasetId: string, name: string) =>
    requestJson<DatasetSnapshot>(`/api/v1/datasets/${datasetId}/sheet`, {
      method: "PUT",
      body: JSON.stringify({ name }),
    }),
  startProfile: (datasetId: string) =>
    requestJson<DatasetSnapshot>(`/api/v1/datasets/${datasetId}/profile`, { method: "POST" }),
  getProfile: (datasetId: string) =>
    requestJson<DatasetProfile>(`/api/v1/datasets/${datasetId}/profile?view=raw`),
  saveSchema: (datasetId: string, columns: ColumnSchema[]) =>
    requestJson<DatasetSnapshot & { schema_version: string }>(
      `/api/v1/datasets/${datasetId}/schema`,
      { method: "PUT", body: JSON.stringify({ columns }) },
    ),
  saveRules: (datasetId: string, rules: RuleSpec[]) =>
    requestJson<DatasetSnapshot & { rules_version: string }>(
      `/api/v1/datasets/${datasetId}/rules`,
      { method: "PUT", body: JSON.stringify({ rules }) },
    ),
  normalize: (datasetId: string) =>
    requestJson<DatasetSnapshot>(`/api/v1/datasets/${datasetId}/normalize`, { method: "POST" }),
  publishPublicMetadata: (payload: Record<string, unknown>) =>
    requestJson<ManifestFile>("/api/v1/privacy/public-metadata", {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  createJob: (request: SynthesisRequest) =>
    requestJson<JobSnapshot>("/api/v1/jobs", {
      method: "POST",
      headers: { "Idempotency-Key": crypto.randomUUID() },
      body: JSON.stringify(request),
    }),
  getJob: (jobId: string) => requestJson<JobSnapshot>(`/api/v1/jobs/${jobId}`),
  cancelJob: (jobId: string) =>
    requestJson<JobSnapshot>(`/api/v1/jobs/${jobId}/cancel`, { method: "POST" }),
  resumeJob: (jobId: string) =>
    requestJson<JobSnapshot>(`/api/v1/jobs/${jobId}/resume`, {
      method: "POST",
      headers: { "Idempotency-Key": crypto.randomUUID() },
    }),
  listJobs: (datasetId?: string, limit = 20) => {
    const params = new URLSearchParams({ limit: String(limit) });
    if (datasetId) params.set("dataset_id", datasetId);
    return requestJson<JobList>(`/api/v1/jobs?${params}`);
  },
  getPrimaryReport: (jobId: string) =>
    requestJson<PrimaryReport>(`/api/v1/jobs/${jobId}/reports/primary`),
  getReleaseReport: (jobId: string) =>
    requestJson<PrimaryReport>(`/api/v1/jobs/${jobId}/reports/release`),
  getArtifacts: (jobId: string) =>
    requestJson<ArtifactList>(`/api/v1/jobs/${jobId}/artifacts?scope=downloadable`),
};
