import { expect, type Page, test } from "@playwright/test";

const DATASET_ID = "11111111-1111-4111-8111-111111111111";
const JOB_ONE = "22222222-2222-4222-8222-222222222222";
const JOB_TWO = "33333333-3333-4333-8333-333333333333";
const MANIFEST_SHA = "a".repeat(64);
const LARGE_LOGICAL_SIZE = 64 * 1024 * 1024 + 17;

interface MockState {
  sourceFormat: "csv" | "xlsx";
  offsets: number[];
  patchBodies: number[];
  completedSha: string | null;
  schemaPayload: Record<string, unknown> | null;
  rulesPayload: Record<string, unknown> | null;
  jobPayload: Record<string, unknown> | null;
}

async function installMemoryAudit(page: Page): Promise<void> {
  await page.addInitScript(() => {
    const audit = {
      arrayBuffer: 0,
      blobConstructor: 0,
      objectUrl: 0,
      slices: [] as Array<{ start: number; end: number }>,
    };
    Object.defineProperty(window, "__studioMemoryAudit", { value: audit });

    const nativeArrayBuffer = Blob.prototype.arrayBuffer;
    Blob.prototype.arrayBuffer = function (...args: Parameters<Blob["arrayBuffer"]>) {
      audit.arrayBuffer += 1;
      return nativeArrayBuffer.apply(this, args);
    };

    const nativeSlice = File.prototype.slice;
    File.prototype.slice = function (start?: number, end?: number, contentType?: string) {
      audit.slices.push({ start: start ?? 0, end: end ?? this.size });
      return nativeSlice.call(this, start, end, contentType);
    };

    const NativeBlob = window.Blob;
    window.Blob = new Proxy(NativeBlob, {
      construct(target, args, newTarget) {
        audit.blobConstructor += 1;
        return Reflect.construct(target, args, newTarget) as Blob;
      },
    });

    const nativeCreateObjectUrl = URL.createObjectURL;
    URL.createObjectURL = function (object: Blob | MediaSource) {
      audit.objectUrl += 1;
      return nativeCreateObjectUrl.call(URL, object);
    };
  });
}

async function mockStudioApi(page: Page, sourceFormat: "csv" | "xlsx"): Promise<MockState> {
  const state: MockState = {
    sourceFormat,
    offsets: [],
    patchBodies: [],
    completedSha: null,
    schemaPayload: null,
    rulesPayload: null,
    jobPayload: null,
  };

  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    const method = request.method();

    if (path === "/api/v1/bootstrap") {
      await route.fulfill({ status: 204, headers: { "Set-Cookie": "sts_session=test; HttpOnly; SameSite=Strict; Path=/" } });
      return;
    }
    if (path === "/api/v1/datasets/uploads" && method === "POST") {
      await route.fulfill({
        status: 201,
        contentType: "application/json",
        body: JSON.stringify({ dataset_id: DATASET_ID, upload_id: DATASET_ID, state: "uploading", upload_offset: 0 }),
      });
      return;
    }
    if (path === `/api/v1/datasets/${DATASET_ID}/content` && method === "HEAD") {
      await route.fulfill({ status: 204, headers: { "Upload-Offset": String(state.offsets.at(-1) ?? 0) } });
      return;
    }
    if (path === `/api/v1/datasets/${DATASET_ID}/content` && method === "PATCH") {
      const offset = Number(request.headers()["upload-offset"]);
      const next = offset === 0 ? Math.min(LARGE_LOGICAL_SIZE, 64 * 1024 * 1024) : LARGE_LOGICAL_SIZE;
      state.offsets.push(next);
      state.patchBodies.push(request.postDataBuffer()?.byteLength ?? 0);
      expect(request.headers()["content-type"]).toBe("application/offset+octet-stream");
      await route.fulfill({ status: 204, headers: { "Upload-Offset": String(next) } });
      return;
    }
    if (path === `/api/v1/datasets/${DATASET_ID}/complete` && method === "POST") {
      state.completedSha = (request.postDataJSON() as { sha256: string }).sha256;
      await route.fulfill({
        status: 202,
        contentType: "application/json",
        body: JSON.stringify({
          dataset_id: DATASET_ID,
          state: sourceFormat === "csv" ? "parse_options_required" : "sheet_required",
        }),
      });
      return;
    }
    if (path === `/api/v1/datasets/${DATASET_ID}/parse-options` && method === "GET") {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          dataset_id: DATASET_ID,
          state: "parse_options_required",
          proposal: {
            sample_size_bytes: 8192,
            candidates: [
              { encoding: "utf-8", delimiter: ",", detected_columns: 1, sampled_records: 20, consistent_records: 20 },
              { encoding: "cp949", delimiter: ";", detected_columns: 3, sampled_records: 20, consistent_records: 20 },
            ],
            recommended: {
              encoding: "cp949",
              delimiter: ";",
              detected_columns: 3,
              sampled_records: 20,
              consistent_records: 20,
            },
            ambiguous: true,
          },
          confirmation: null,
          malformed_preview: [{ logical_record: 14 }],
        }),
      });
      return;
    }
    if (path === `/api/v1/datasets/${DATASET_ID}/parse-options` && method === "PUT") {
      await route.fulfill({ contentType: "application/json", body: JSON.stringify({ dataset_id: DATASET_ID, state: "raw_ready" }) });
      return;
    }
    if (path === `/api/v1/datasets/${DATASET_ID}/sheets` && method === "GET") {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          dataset_id: DATASET_ID,
          state: "sheet_required",
          requires_sheet_selection: true,
          selected_sheet: null,
          sheets: [
            { name: "요약", rows: 24, columns: 3 },
            { name: "원본 데이터", rows: 1200, columns: 3 },
          ],
        }),
      });
      return;
    }
    if (path === `/api/v1/datasets/${DATASET_ID}/sheet` && method === "PUT") {
      expect(request.postDataJSON()).toEqual({ name: "원본 데이터" });
      await route.fulfill({ contentType: "application/json", body: JSON.stringify({ dataset_id: DATASET_ID, state: "raw_ready" }) });
      return;
    }
    if (path === `/api/v1/datasets/${DATASET_ID}/profile` && method === "POST") {
      await route.fulfill({ status: 202, contentType: "application/json", body: JSON.stringify({ dataset_id: DATASET_ID, state: "profiled" }) });
      return;
    }
    if (path === `/api/v1/datasets/${DATASET_ID}/profile` && method === "GET") {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          version: "1.0",
          view: "raw",
          row_count: 1200,
          column_count: 3,
          columns: [
            { name: "code", storage_type: "VARCHAR", row_count: 1200, null_count: 0, nonnull_count: 1200, minimum: "001", maximum: "999", approx_cardinality: 1100, parse_success: { integer: 1200, float: 1200, boolean: 0, date: 0, datetime: 0 }, candidate_type: "integer", candidate_requires_confirmation: true, candidate_alternatives: ["categorical", "identifier"] },
            { name: "age", storage_type: "VARCHAR", row_count: 1200, null_count: 4, nonnull_count: 1196, minimum: "18", maximum: "89", approx_cardinality: 72, parse_success: { integer: 1196, float: 1196, boolean: 0, date: 0, datetime: 0 }, candidate_type: "integer" },
            { name: "city", storage_type: "VARCHAR", row_count: 1200, null_count: 0, nonnull_count: 1200, minimum: "Busan", maximum: "Seoul", approx_cardinality: 8, parse_success: { integer: 0, float: 0, boolean: 0, date: 0, datetime: 0 }, candidate_type: "categorical" },
          ],
        }),
      });
      return;
    }
    if (path === `/api/v1/datasets/${DATASET_ID}/schema` && method === "PUT") {
      state.schemaPayload = request.postDataJSON() as Record<string, unknown>;
      await route.fulfill({ contentType: "application/json", body: JSON.stringify({ dataset_id: DATASET_ID, state: "schema_ready", schema_version: "schema-v1" }) });
      return;
    }
    if (path === `/api/v1/datasets/${DATASET_ID}/rules` && method === "PUT") {
      state.rulesPayload = request.postDataJSON() as Record<string, unknown>;
      await route.fulfill({ contentType: "application/json", body: JSON.stringify({ dataset_id: DATASET_ID, state: "schema_ready", rules_version: "rules-v1" }) });
      return;
    }
    if (path === `/api/v1/datasets/${DATASET_ID}/normalize` && method === "POST") {
      await route.fulfill({ status: 202, contentType: "application/json", body: JSON.stringify({ dataset_id: DATASET_ID, state: "normalized" }) });
      return;
    }
    if (path === `/api/v1/datasets/${DATASET_ID}` && method === "GET") {
      await route.fulfill({ contentType: "application/json", body: JSON.stringify({ dataset_id: DATASET_ID, state: "normalized", manifest_sha256: MANIFEST_SHA, legal_actions: [] }) });
      return;
    }
    if (path === "/api/v1/jobs" && method === "POST") {
      state.jobPayload = request.postDataJSON() as Record<string, unknown>;
      await route.fulfill({
        status: 201,
        contentType: "application/json",
        body: JSON.stringify({ job_id: JOB_ONE, dataset_id: DATASET_ID, state: "fitting", attempt: 1, legal_actions: ["cancel"], progress: { stage: "fitting", state: "fitting", completed: 1, total: 4, unit: "단계" } }),
      });
      return;
    }
    if (path === `/api/v1/jobs/${JOB_ONE}/events`) {
      await route.fulfill({
        status: 200,
        contentType: "text/event-stream",
        headers: { "Cache-Control": "no-cache" },
        body: `id: 1\ndata: ${JSON.stringify({ stage: "generating", state: "generating", completed: 2, total: 4, unit: "단계", message_code: "JOB_GENERATING" })}\n\n`,
      });
      return;
    }
    if (path === `/api/v1/jobs/${JOB_ONE}/cancel` && method === "POST") {
      await route.fulfill({ status: 202, contentType: "application/json", body: JSON.stringify({ job_id: JOB_ONE, dataset_id: DATASET_ID, state: "cancelled", attempt: 1, resume_boundary: "validated_fit_checkpoint", legal_actions: ["resume"] }) });
      return;
    }
    if (path === `/api/v1/jobs/${JOB_ONE}/resume` && method === "POST") {
      await route.fulfill({ status: 201, contentType: "application/json", body: JSON.stringify({ job_id: JOB_TWO, dataset_id: DATASET_ID, state: "generating", attempt: 1, retry_of: JOB_ONE, legal_actions: ["cancel"] }) });
      return;
    }
    if (path === `/api/v1/jobs/${JOB_TWO}/events`) {
      await route.fulfill({
        status: 200,
        contentType: "text/event-stream",
        body: `id: 8\ndata: ${JSON.stringify({ stage: "publishing", state: "succeeded", completed: 7, total: 7, unit: "단계", message_code: "JOB_SUCCEEDED" })}\n\n`,
      });
      return;
    }
    if (path === `/api/v1/jobs/${JOB_TWO}/reports/primary`) {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          version: "1.0",
          report_kind: "utility_primary",
          narrative: ["The downloadable report includes a metric-based narrative."],
          evaluation: {
            summary: { requested_rows: 100000, actual_rows: 100000, median_excess: 0.021, p95_excess: 0.074 },
            columns: [
              { name: "age", metric: "KS", distance: 0.08, baseline_excess: 0.02, missingness_difference: 0.003 },
              { name: "city", metric: "TVD", distance: 0.11, baseline_excess: 0.04, missingness_difference: 0 },
            ],
          },
        }),
      });
      return;
    }
    if (path === `/api/v1/jobs/${JOB_TWO}/artifacts`) {
      await route.fulfill({
        contentType: "application/json",
        body: JSON.stringify({
          job_id: JOB_TWO,
          scope: "downloadable",
          artifacts: [
            { artifact_id: "44444444-4444-4444-8444-444444444444", kind: "synthetic_parquet_zip", size_bytes: 845102, downloadable: true, release_safe: false, contains_private_source_information: false },
            { artifact_id: "55555555-5555-4555-8555-555555555555", kind: "primary_report_html", size_bytes: 28103, downloadable: true, release_safe: false, contains_private_source_information: true },
          ],
        }),
      });
      return;
    }
    if (path === `/api/v1/jobs/${JOB_ONE}` && method === "GET") {
      await route.fulfill({ contentType: "application/json", body: JSON.stringify({ job_id: JOB_ONE, dataset_id: DATASET_ID, state: "cancelled", legal_actions: ["resume"], resume_boundary: "validated_fit_checkpoint" }) });
      return;
    }
    await route.fulfill({ status: 404, contentType: "application/problem+json", body: JSON.stringify({ code: "NOT_MOCKED", detail: `${method} ${path}` }) });
  });
  return state;
}

async function chooseLogicalLargeFile(page: Page, name: string, mimeType: string): Promise<void> {
  await page.locator("#source-file").setInputFiles({
    name,
    mimeType,
    buffer: Buffer.from("code,age,city\n001,20,Seoul\n"),
  });
  await page.locator("#source-file").evaluate((input, logicalSize) => {
    const selected = (input as HTMLInputElement).files?.[0];
    if (!selected) throw new Error("test file was not selected");
    Object.defineProperty(selected, "size", { configurable: true, value: logicalSize });
  }, LARGE_LOGICAL_SIZE);
}

test("CSV six-step flow uploads chunks, resolves conflicts, resumes, and reports", async ({ page }) => {
  await page.setViewportSize({ width: 1280, height: 800 });
  await installMemoryAudit(page);
  const state = await mockStudioApi(page, "csv");
  await page.goto("/");

  await expect(page.getByRole("status")).toContainText("준비되었습니다");
  await page.keyboard.press("Tab");
  await expect(page.getByRole("link", { name: /본문으로 건너뛰기/ })).toBeFocused();

  await chooseLogicalLargeFile(page, "people.csv", "text/csv");
  await page.getByRole("button", { name: "업로드하고 검사" }).click();
  await expect(page.getByRole("heading", { name: "CSV 읽기 방식 확인" })).toBeVisible();
  await expect(page.getByLabel("인코딩")).toHaveValue("cp949");
  await expect(page.getByLabel("구분자")).toHaveValue(";");
  await expect(page.getByText(/미리보기 경고/)).toBeVisible();
  await page.getByRole("button", { name: "이 방식으로 계속" }).click();

  await expect(page.getByRole("heading", { name: "열 스키마 확인" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "열 스키마 확인" })).toBeFocused();
  await expect(page.getByText("확인 필요")).toBeVisible();
  await page.getByLabel("code 유형").selectOption("categorical");
  await page.getByRole("button", { name: "스키마 저장" }).click();

  await expect(page.getByRole("heading", { name: "무결성 규칙 정의" })).toBeVisible();
  await expect(page.getByLabel("규칙 형태").locator("option")).toHaveCount(8);
  await page.getByLabel("규칙 형태").selectOption("mask_prefix");
  await page.getByLabel("대상 열").selectOption("age");
  await page.getByLabel("유지할 앞 문자 수").fill("1");
  await page.getByRole("button", { name: "규칙 추가" }).click();
  await page.getByLabel("규칙 형태").selectOption("conditional_set");
  await page.getByLabel("조건 열").selectOption("city");
  await page.getByLabel("조건 값").fill("Seoul");
  await page.getByLabel("고정할 대상 열").selectOption("age");
  await page.getByRole("textbox", { name: "고정값", exact: true }).fill("30");
  await page.getByRole("button", { name: "규칙 추가" }).click();
  await expect(page.getByRole("alert").filter({ hasText: "규칙 충돌" })).toContainText("모두 값을 씁니다");
  await expect(page.getByRole("button", { name: "규칙 저장하고 정규화" })).toBeDisabled();
  await page.getByRole("button", { name: "조건부 고정값 규칙 삭제" }).click();
  await page.getByRole("button", { name: "규칙 저장하고 정규화" }).click();

  await expect(page.getByRole("heading", { name: "생성 모드와 자원" })).toBeVisible();
  await expect(page.getByRole("radio", { name: /형식적 차등프라이버시/ })).toBeDisabled();
  await expect(page.locator("#dp-audit")).toContainText("공개 sampling seed");
  await expect(page.getByText(/수학적 보호 보장은 없습니다/).first()).toBeVisible();
  await page.getByRole("checkbox", { name: "CSV" }).check();
  await page.getByRole("button", { name: "일반 합성 시작" }).click();

  await expect(page.getByRole("heading", { name: "합성 작업 진행" })).toBeVisible();
  await expect(page.getByRole("progressbar", { name: "합성 작업 진행률" })).toHaveAttribute("value", "50");
  await page.getByRole("button", { name: "작업 취소" }).click();
  await expect(page.getByRole("button", { name: "새 작업으로 재개" })).toBeVisible();
  await page.getByRole("button", { name: "새 작업으로 재개" }).click();

  await expect(page.getByRole("heading", { name: "품질 보고서와 산출물" })).toBeVisible();
  await expect(page.getByText("개인정보 보호 보장 없음")).toBeVisible();
  await expect(page.getByTestId("report-chart")).toBeVisible();
  await expect(page.getByRole("heading", { name: "보고서 해설" })).toBeVisible();
  await expect(page.getByText(/100,000행을 요청했고 실제 100,000행/)).toBeVisible();
  const download = page.getByRole("link", { name: "파일 받기" }).first();
  await expect(download).toHaveAttribute("href", "/api/v1/artifacts/44444444-4444-4444-8444-444444444444/download");
  await expect(download).not.toHaveAttribute("download", /.+/);

  const summaryTab = page.getByRole("tab", { name: "품질 요약" });
  await summaryTab.focus();
  await page.keyboard.press("ArrowRight");
  await expect(page.getByRole("tab", { name: "열별 거리" })).toHaveAttribute("aria-selected", "true");
  await expect(page.getByRole("tab", { name: "열별 거리" })).toBeFocused();
  await expect(page.getByRole("columnheader", { name: "합성 거리" })).toBeVisible();

  expect(state.offsets).toEqual([64 * 1024 * 1024, LARGE_LOGICAL_SIZE]);
  expect(state.patchBodies).toHaveLength(2);
  expect(state.completedSha).toBe("d4243e1b3fa0c28b0296c2c4c07f24101a4af0f4c43eb680e0ee2afc33220478");
  expect(state.schemaPayload).toMatchObject({ columns: [
    { name: "code", kind: "categorical" },
    { name: "age", kind: "integer" },
    { name: "city", kind: "categorical" },
  ] });
  expect(state.rulesPayload).toMatchObject({ rules: [{ kind: "mask_prefix", column: "age" }] });
  expect(state.jobPayload).toMatchObject({ mode: "utility", synthesizer: "tabular_argn", output_formats: ["parquet", "csv"] });

  const audit = await page.evaluate(() => (window as unknown as { __studioMemoryAudit: { arrayBuffer: number; blobConstructor: number; objectUrl: number; slices: Array<{ start: number; end: number }> } }).__studioMemoryAudit);
  expect(audit.arrayBuffer).toBe(0);
  expect(audit.blobConstructor).toBe(0);
  expect(audit.objectUrl).toBe(0);
  expect(audit.slices).toEqual([
    { start: 0, end: 64 * 1024 * 1024 },
    { start: 64 * 1024 * 1024, end: LARGE_LOGICAL_SIZE },
  ]);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("XLSX sheet branch is keyboard-operable and responsive", async ({ page }) => {
  await page.setViewportSize({ width: 768, height: 900 });
  await installMemoryAudit(page);
  await mockStudioApi(page, "xlsx");
  await page.goto("/");
  await expect(page.getByRole("status")).toContainText("준비되었습니다");

  await page.keyboard.press("Tab");
  await expect(page.getByRole("link", { name: /본문으로 건너뛰기/ })).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("main")).toBeFocused();

  await chooseLogicalLargeFile(page, "workbook.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
  await page.getByRole("button", { name: "업로드하고 검사" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("heading", { name: "XLSX 시트 선택" })).toBeVisible();
  const sheetSelect = page.getByLabel("처리할 시트");
  await sheetSelect.selectOption("원본 데이터");
  await page.getByRole("button", { name: "이 시트로 계속" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("heading", { name: "열 스키마 확인" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "열 스키마 확인" })).toBeFocused();
  await expect(page.locator(".step-rail li")).toHaveCount(6);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);

  const audit = await page.evaluate(() => (window as unknown as { __studioMemoryAudit: { arrayBuffer: number; blobConstructor: number; objectUrl: number } }).__studioMemoryAudit);
  expect(audit.arrayBuffer).toBe(0);
  expect(audit.blobConstructor).toBe(0);
  expect(audit.objectUrl).toBe(0);
});
