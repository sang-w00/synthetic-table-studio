import { readFile, stat, writeFile } from "node:fs/promises";

import { expect, test } from "@playwright/test";
import type {
  Page,
  Request as PlaywrightRequest,
  Response as PlaywrightResponse,
} from "@playwright/test";

interface ApiObservation {
  method: string;
  path: string;
  request: PlaywrightRequest;
  response: PlaywrightResponse;
}

interface DatasetUploadResponse {
  dataset_id: string;
  state: string;
}

interface JobResponse {
  job_id: string;
  state: string;
  retry_of?: string | null;
  resume_boundary?: string | null;
  legal_actions?: string[];
}

interface ZipMember {
  name: string;
  content: string;
}

const CRC32_TABLE = Array.from({ length: 256 }, (_, index) => {
  let value = index;
  for (let bit = 0; bit < 8; bit += 1) {
    value = (value & 1) === 1 ? 0xedb88320 ^ (value >>> 1) : value >>> 1;
  }
  return value >>> 0;
});

function observeApi(page: Page): ApiObservation[] {
  const observations: ApiObservation[] = [];
  page.on("response", (response) => {
    const url = new URL(response.url());
    if (!url.pathname.startsWith("/api/v1/")) return;
    const request = response.request();
    observations.push({
      method: request.method(),
      path: url.pathname,
      request,
      response,
    });
  });
  return observations;
}

function matchingObservations(
  observations: ApiObservation[],
  method: string,
  path: string | RegExp,
): ApiObservation[] {
  return observations.filter(
    (observation) =>
      observation.method === method &&
      (typeof path === "string" ? observation.path === path : path.test(observation.path)),
  );
}

function requireObservation(
  observations: ApiObservation[],
  method: string,
  path: string | RegExp,
  boundary: string,
): ApiObservation {
  const observation = matchingObservations(observations, method, path).at(-1);
  if (!observation) {
    throw new Error(
      `${boundary}: ${method} ${String(path)} was never observed; ` +
        `observed=${observations.map((item) => `${item.method} ${item.path}`).join(", ")}`,
    );
  }
  return observation;
}

async function expectApiStatus(
  observation: ApiObservation,
  expectedStatus: number,
  boundary: string,
): Promise<void> {
  let failureBody = "";
  if (observation.response.status() !== expectedStatus) {
    failureBody = await observation.response.text().catch((error: unknown) =>
      error instanceof Error ? `<unreadable: ${error.message}>` : "<unreadable>",
    );
  }
  expect(
    observation.response.status(),
    `${boundary}: ${observation.method} ${observation.path} ` +
      `returned ${observation.response.status()}, body=${failureBody}`,
  ).toBe(expectedStatus);
}

async function openBootstrappedStudio(page: Page): Promise<void> {
  const bootstrapResponse = page.waitForResponse((response) => {
    const url = new URL(response.url());
    return url.pathname === "/api/v1/bootstrap" && response.request().method() === "GET";
  });
  const navigation = await page.goto("/");
  expect(navigation, "scripts/serve did not return a document response for GET /").not.toBeNull();
  expect(navigation?.status(), "scripts/serve GET / must serve the production web build").toBe(200);
  expect(navigation?.headers()["content-security-policy"]).toContain("default-src 'self'");

  const bootstrap = await bootstrapResponse;
  expect(bootstrap.status(), "GET /api/v1/bootstrap must establish the host-only session").toBe(200);
  await expect(page.getByRole("status")).toContainText("준비되었습니다");

  const cookies = await page.context().cookies();
  expect(
    cookies.some((cookie) => cookie.httpOnly && cookie.sameSite === "Strict" && cookie.path === "/"),
    "bootstrap must set an HttpOnly, SameSite=Strict, host-only session cookie",
  ).toBe(true);
}

async function expectNoHorizontalOverflow(page: Page, viewport: string): Promise<void> {
  expect(
    await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth),
    `${viewport} layout must not create horizontal document overflow`,
  ).toBe(true);
}

async function assertResumableUpload(
  observations: ApiObservation[],
  datasetId: string,
  fileSize: number,
): Promise<void> {
  const escapedDatasetId = datasetId.replaceAll("-", "\\-");
  const contentPath = new RegExp(`^/api/v1/datasets/${escapedDatasetId}/content$`);
  const head = requireObservation(
    observations,
    "HEAD",
    contentPath,
    "resumable upload offset recovery",
  );
  await expectApiStatus(head, 204, "resumable upload offset recovery");
  expect(head.response.headers()["upload-offset"]).toBe("0");

  const patches = matchingObservations(observations, "PATCH", contentPath);
  expect(patches.length, "real file upload must send at least one PATCH chunk").toBeGreaterThan(0);
  let acknowledgedOffset = 0;
  for (const [index, patch] of patches.entries()) {
    await expectApiStatus(patch, 204, `resumable upload PATCH chunk ${index + 1}`);
    expect(
      Number(patch.request.headers()["upload-offset"]),
      `PATCH chunk ${index + 1} must continue from the last acknowledged server offset`,
    ).toBe(acknowledgedOffset);
    acknowledgedOffset = Number(patch.response.headers()["upload-offset"]);
    expect(
      Number.isSafeInteger(acknowledgedOffset) && acknowledgedOffset > 0,
      `PATCH chunk ${index + 1} must return a forward Upload-Offset`,
    ).toBe(true);
  }
  expect(acknowledgedOffset, "the final server Upload-Offset must equal the real file size").toBe(
    fileSize,
  );
}

async function writeAmbiguousCsv(path: string): Promise<number> {
  const segments = ["A", "B", "C", "D"];
  const rows = Array.from(
    { length: 1_200 },
    (_, index) => `Row${index},${18 + (index % 60)},${segments[index % segments.length]};;`,
  );
  const content = `Index,age,segment;;\n${rows.join("\n")}\n`;
  await writeFile(path, content, "utf8");
  return Buffer.byteLength(content);
}

function crc32(content: Buffer): number {
  let checksum = 0xffffffff;
  for (const byte of content) {
    checksum = CRC32_TABLE[(checksum ^ byte) & 0xff] ^ (checksum >>> 8);
  }
  return (checksum ^ 0xffffffff) >>> 0;
}

function storedZip(members: ZipMember[]): Buffer {
  const localParts: Buffer[] = [];
  const centralParts: Buffer[] = [];
  const utf8Flag = 0x0800;
  const dosTime = 0;
  const dosDate = ((2026 - 1980) << 9) | (7 << 5) | 23;
  let localOffset = 0;

  for (const member of members) {
    const name = Buffer.from(member.name, "utf8");
    const content = Buffer.from(member.content, "utf8");
    const checksum = crc32(content);
    const localHeader = Buffer.alloc(30);
    localHeader.writeUInt32LE(0x04034b50, 0);
    localHeader.writeUInt16LE(20, 4);
    localHeader.writeUInt16LE(utf8Flag, 6);
    localHeader.writeUInt16LE(0, 8);
    localHeader.writeUInt16LE(dosTime, 10);
    localHeader.writeUInt16LE(dosDate, 12);
    localHeader.writeUInt32LE(checksum, 14);
    localHeader.writeUInt32LE(content.length, 18);
    localHeader.writeUInt32LE(content.length, 22);
    localHeader.writeUInt16LE(name.length, 26);
    localHeader.writeUInt16LE(0, 28);
    localParts.push(localHeader, name, content);

    const centralHeader = Buffer.alloc(46);
    centralHeader.writeUInt32LE(0x02014b50, 0);
    centralHeader.writeUInt16LE(20, 4);
    centralHeader.writeUInt16LE(20, 6);
    centralHeader.writeUInt16LE(utf8Flag, 8);
    centralHeader.writeUInt16LE(0, 10);
    centralHeader.writeUInt16LE(dosTime, 12);
    centralHeader.writeUInt16LE(dosDate, 14);
    centralHeader.writeUInt32LE(checksum, 16);
    centralHeader.writeUInt32LE(content.length, 20);
    centralHeader.writeUInt32LE(content.length, 24);
    centralHeader.writeUInt16LE(name.length, 28);
    centralHeader.writeUInt16LE(0, 30);
    centralHeader.writeUInt16LE(0, 32);
    centralHeader.writeUInt16LE(0, 34);
    centralHeader.writeUInt16LE(0, 36);
    centralHeader.writeUInt32LE(0, 38);
    centralHeader.writeUInt32LE(localOffset, 42);
    centralParts.push(centralHeader, name);
    localOffset += localHeader.length + name.length + content.length;
  }

  const centralDirectory = Buffer.concat(centralParts);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(0, 4);
  end.writeUInt16LE(0, 6);
  end.writeUInt16LE(members.length, 8);
  end.writeUInt16LE(members.length, 10);
  end.writeUInt32LE(centralDirectory.length, 12);
  end.writeUInt32LE(localOffset, 16);
  end.writeUInt16LE(0, 20);
  return Buffer.concat([...localParts, centralDirectory, end]);
}

function escapeXml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&apos;");
}

function worksheetXml(rows: string[][]): string {
  const body = rows
    .map((row, rowIndex) => {
      const cells = row
        .map((value, columnIndex) => {
          const column = String.fromCharCode("A".charCodeAt(0) + columnIndex);
          return `<c r="${column}${rowIndex + 1}" t="inlineStr"><is><t>${escapeXml(value)}</t></is></c>`;
        })
        .join("");
      return `<row r="${rowIndex + 1}">${cells}</row>`;
    })
    .join("");
  return (
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
    '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">' +
    `<sheetData>${body}</sheetData></worksheet>`
  );
}

async function writeXlsxFixture(path: string): Promise<number> {
  const contentTypes =
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
    '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">' +
    '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>' +
    '<Default Extension="xml" ContentType="application/xml"/>' +
    '<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>' +
    '<Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>' +
    '<Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>' +
    "</Types>";
  const rootRelationships =
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' +
    '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>' +
    "</Relationships>";
  const workbook =
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
    '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">' +
    '<sheets><sheet name="원본 데이터" sheetId="1" r:id="rId1"/>' +
    '<sheet name="검증 데이터" sheetId="2" r:id="rId2"/></sheets></workbook>';
  const workbookRelationships =
    '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' +
    '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">' +
    '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>' +
    '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>' +
    "</Relationships>";
  const fixture = storedZip([
    { name: "[Content_Types].xml", content: contentTypes },
    { name: "_rels/.rels", content: rootRelationships },
    { name: "xl/workbook.xml", content: workbook },
    { name: "xl/_rels/workbook.xml.rels", content: workbookRelationships },
    {
      name: "xl/worksheets/sheet1.xml",
      content: worksheetXml([
        ["id", "group"],
        ["1", "source"],
        ["2", "source"],
      ]),
    },
    {
      name: "xl/worksheets/sheet2.xml",
      content: worksheetXml([
        ["id", "group"],
        ["10", "validation"],
        ["11", "validation"],
      ]),
    },
  ]);
  await writeFile(path, fixture);
  return fixture.length;
}

test("@desktop real CSV lifecycle reaches a utility report, cancel/resume, and link download", async ({
  page,
}, testInfo) => {
  const observations = observeApi(page);
  await openBootstrappedStudio(page);
  await expect(page).toHaveTitle("Synthetic Table Studio");
  await expect(page.locator('.step-rail button[aria-current="step"]')).toContainText("업로드");

  await page.keyboard.press("Tab");
  const skipLink = page.getByRole("link", { name: /본문으로 건너뛰기/ });
  await expect(skipLink).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("main")).toBeFocused();

  const csvPath = testInfo.outputPath("real-resumable.csv");
  const csvSize = await writeAmbiguousCsv(csvPath);
  const fileInput = page.getByLabel("CSV 또는 XLSX 파일");
  await fileInput.setInputFiles(csvPath);
  await expect(page.getByText("real-resumable.csv", { exact: true })).toBeVisible();

  const uploadButton = page.getByRole("button", { name: "업로드하고 검사" });
  await uploadButton.focus();
  await expect(uploadButton).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("heading", { name: "CSV 읽기 방식 확인" })).toBeVisible({
    timeout: 120_000,
  });

  const uploadCreation = requireObservation(
    observations,
    "POST",
    "/api/v1/datasets/uploads",
    "CSV upload creation",
  );
  await expectApiStatus(uploadCreation, 201, "CSV upload creation");
  const upload = (await uploadCreation.response.json()) as DatasetUploadResponse;
  expect(upload.state).toBe("uploading");
  await assertResumableUpload(observations, upload.dataset_id, csvSize);
  const completePath = `/api/v1/datasets/${upload.dataset_id}/complete`;
  const completion = requireObservation(observations, "POST", completePath, "CSV inspection");
  await expectApiStatus(completion, 202, "CSV inspection");
  expect(((await completion.response.json()) as DatasetUploadResponse).state).toBe(
    "parse_options_required",
  );
  await expectApiStatus(
    requireObservation(
      observations,
      "GET",
      `/api/v1/datasets/${upload.dataset_id}/parse-options`,
      "CSV parse proposal",
    ),
    200,
    "CSV parse proposal",
  );
  await expect(page.getByLabel("파일 업로드 진행률")).toHaveAttribute("value", "100");
  await expect(page.getByLabel("인코딩")).toHaveValue("utf-8");
  await expect(page.getByLabel("구분자")).toHaveValue(",");

  const parseButton = page.getByRole("button", { name: "이 방식으로 계속" });
  await parseButton.focus();
  await page.keyboard.press("Enter");
  const schemaHeading = page.getByRole("heading", { name: "열 스키마 확인" });
  await expect(schemaHeading).toBeVisible({ timeout: 120_000 });
  await expect(schemaHeading).toBeFocused();
  await expectApiStatus(
    requireObservation(
      observations,
      "PUT",
      `/api/v1/datasets/${upload.dataset_id}/parse-options`,
      "confirmed CSV parse",
    ),
    200,
    "confirmed CSV parse",
  );
  await expectApiStatus(
    requireObservation(
      observations,
      "POST",
      `/api/v1/datasets/${upload.dataset_id}/profile`,
      "real raw profile",
    ),
    202,
    "real raw profile",
  );
  await expectApiStatus(
    requireObservation(
      observations,
      "GET",
      `/api/v1/datasets/${upload.dataset_id}/profile`,
      "real raw profile result",
    ),
    200,
    "real raw profile result",
  );

  await expect(page.getByLabel("Index 유형")).toHaveValue("identifier");
  await expect(page.getByText("확인 필요").first()).toBeVisible();
  await page.getByLabel("age 유형").selectOption("integer");
  await page.getByLabel("segment;; 유형").selectOption("categorical");
  const saveSchema = page.getByRole("button", { name: "스키마 저장" });
  await saveSchema.focus();
  await page.keyboard.press("Enter");
  const rulesHeading = page.getByRole("heading", { name: "무결성 규칙 정의" });
  await expect(rulesHeading).toBeVisible();
  await expect(rulesHeading).toBeFocused();
  await expectApiStatus(
    requireObservation(
      observations,
      "PUT",
      `/api/v1/datasets/${upload.dataset_id}/schema`,
      "schema_ready transition",
    ),
    200,
    "schema_ready transition",
  );

  await page.getByLabel("규칙 형태").selectOption("not_null");
  await page.getByLabel("대상 열").selectOption("age");
  const addRule = page.getByRole("button", { name: "규칙 추가" });
  await addRule.focus();
  await page.keyboard.press("Enter");
  await expect(page.locator(".rule-list")).toContainText("age · null 금지");

  const normalizeButton = page.getByRole("button", { name: "규칙 저장하고 정규화" });
  await normalizeButton.focus();
  await page.keyboard.press("Enter");
  const modeHeading = page.getByRole("heading", { name: "생성 모드와 자원" });
  await expect(modeHeading).toBeVisible({ timeout: 120_000 });
  await expect(modeHeading).toBeFocused();
  await expectApiStatus(
    requireObservation(
      observations,
      "PUT",
      `/api/v1/datasets/${upload.dataset_id}/rules`,
      "rules compile at schema_ready",
    ),
    200,
    "rules compile at schema_ready",
  );
  await expectApiStatus(
    requireObservation(
      observations,
      "POST",
      `/api/v1/datasets/${upload.dataset_id}/normalize`,
      "schema_ready to normalized transition",
    ),
    202,
    "schema_ready to normalized transition",
  );
  const datasetSnapshot = requireObservation(
    observations,
    "GET",
    `/api/v1/datasets/${upload.dataset_id}`,
    "normalized manifest snapshot",
  );
  await expectApiStatus(datasetSnapshot, 200, "normalized manifest snapshot");
  expect(((await datasetSnapshot.response.json()) as DatasetUploadResponse).state).toBe("normalized");

  const utilityRadio = page.getByRole("radio", { name: /일반 고품질 합성/ });
  const dpRadio = page.getByRole("radio", { name: /형식적 차등프라이버시/ });
  await expect(utilityRadio).toBeChecked();
  await expect(dpRadio).toBeDisabled();
  await expect(page.locator("label.mode-card.disabled")).toContainText(
    "trusted curator",
  );
  await expect(page.getByText("DP 공개 경계란?")).toBeVisible();

  const parquetFormat = page.getByLabel("Parquet shards + ZIP64 manifest");
  const csvFormat = page.getByLabel("CSV", { exact: true });
  const startJob = page.getByRole("button", { name: "일반 합성 시작" });
  await parquetFormat.uncheck();
  await expect(startJob).toBeDisabled();
  await csvFormat.check();
  await parquetFormat.check();
  await expect(startJob).toBeEnabled();
  await page.getByLabel("출력 행 수").fill("2000");
  await page.getByLabel("학습 표본 상한").fill("500");
  await page.getByLabel("최대 epoch").fill("1");
  await page.getByLabel("최대 학습 시간 (분)").fill("5");

  const createJobResponse = page.waitForResponse((response) => {
    const url = new URL(response.url());
    return url.pathname === "/api/v1/jobs" && response.request().method() === "POST";
  });
  await startJob.focus();
  await page.keyboard.press("Enter");
  const createJob = await createJobResponse;
  const createObservation: ApiObservation = {
    method: "POST",
    path: "/api/v1/jobs",
    request: createJob.request(),
    response: createJob,
  };
  await expectApiStatus(createObservation, 201, "normalized utility job creation");
  const originalJob = (await createJob.json()) as JobResponse;
  expect(
    ["queued", "admitted", "preparing", "fitting"],
    `POST /api/v1/jobs returned non-cancellable state ${originalJob.state}`,
  ).toContain(originalJob.state);
  expect(originalJob.legal_actions).toContain("cancel");

  const progressHeading = page.getByRole("heading", { name: "합성 작업 진행" });
  await expect(progressHeading).toBeVisible();
  await expect(progressHeading).toBeFocused();
  await expect(page.getByLabel("합성 작업 진행률")).toBeVisible();
  await expect(page.getByRole("list", { name: "서버 처리 단계" })).toContainText("데이터 준비");

  const cancelResponse = page.waitForResponse((response) => {
    const url = new URL(response.url());
    return (
      url.pathname === `/api/v1/jobs/${originalJob.job_id}/cancel` &&
      response.request().method() === "POST"
    );
  });
  const cancelButton = page.getByRole("button", { name: "작업 취소" });
  await cancelButton.focus();
  await page.keyboard.press("Enter");
  const cancelled = await cancelResponse;
  const cancelObservation: ApiObservation = {
    method: "POST",
    path: `/api/v1/jobs/${originalJob.job_id}/cancel`,
    request: cancelled.request(),
    response: cancelled,
  };
  await expectApiStatus(cancelObservation, 202, "running utility cancellation");
  expect(((await cancelled.json()) as JobResponse).state).toBe("cancelling");

  const resumeButton = page.getByRole("button", { name: "새 작업으로 재개" });
  await expect(resumeButton).toBeVisible({ timeout: 120_000 });
  await expect(page.locator(".job-overview")).toContainText("취소됨");
  const resumeResponse = page.waitForResponse((response) => {
    const url = new URL(response.url());
    return (
      url.pathname === `/api/v1/jobs/${originalJob.job_id}/resume` &&
      response.request().method() === "POST"
    );
  });
  await resumeButton.focus();
  await page.keyboard.press("Enter");
  const resumed = await resumeResponse;
  const resumeObservation: ApiObservation = {
    method: "POST",
    path: `/api/v1/jobs/${originalJob.job_id}/resume`,
    request: resumed.request(),
    response: resumed,
  };
  await expectApiStatus(resumeObservation, 201, "cancelled utility resume boundary");
  const resumedJob = (await resumed.json()) as JobResponse;
  expect(resumedJob.job_id).not.toBe(originalJob.job_id);
  expect(resumedJob.retry_of).toBe(originalJob.job_id);
  expect(
    resumedJob.resume_boundary,
    "resume must name the immutable boundary used for the new job",
  ).toBeTruthy();

  const reportHeading = page.getByRole("heading", { name: "품질 보고서와 산출물" });
  await expect(reportHeading).toBeVisible({ timeout: 8 * 60_000 });
  await expect(reportHeading).toBeFocused();
  await expectApiStatus(
    requireObservation(
      observations,
      "GET",
      `/api/v1/jobs/${resumedJob.job_id}/reports/primary`,
      "succeeded utility primary report",
    ),
    200,
    "succeeded utility primary report",
  );
  await expectApiStatus(
    requireObservation(
      observations,
      "GET",
      `/api/v1/jobs/${resumedJob.job_id}/artifacts`,
      "succeeded utility downloadable artifacts",
    ),
    200,
    "succeeded utility downloadable artifacts",
  );
  await expect(page.getByText("개인정보 보호 보장 없음", { exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "보고서 해설" })).toBeVisible();
  await expect(page.getByText(/2,000행을 요청했고 실제 2,000행/)).toBeVisible();
  await expect(page.getByText(/열별 기준선 초과 거리는 중앙값/)).not.toContainText(
    "계산되지 않음",
  );

  const summaryTab = page.getByRole("tab", { name: "품질 요약" });
  const columnsTab = page.getByRole("tab", { name: "열별 거리" });
  const boundaryTab = page.getByRole("tab", { name: "공개 경계" });
  await summaryTab.focus();
  await page.keyboard.press("ArrowRight");
  await expect(columnsTab).toHaveAttribute("aria-selected", "true");
  await expect(columnsTab).toBeFocused();
  await expect(page.getByRole("tabpanel", { name: "열별 거리" })).toBeVisible();
  await page.keyboard.press("ArrowRight");
  await expect(boundaryTab).toHaveAttribute("aria-selected", "true");
  await expect(boundaryTab).toBeFocused();
  await expect(page.getByRole("tabpanel", { name: "공개 경계" })).toContainText(
    "release_safe=false",
  );

  const csvArtifact = page.locator(".downloads li").filter({ hasText: "합성 CSV" });
  const downloadLink = csvArtifact.getByRole("link", { name: "파일 받기" });
  await expect(downloadLink).toHaveAttribute(
    "href",
    /^\/api\/v1\/artifacts\/[0-9a-f-]+\/download$/,
  );
  const href = await downloadLink.getAttribute("href");
  expect(href).not.toContain("blob:");
  expect(await downloadLink.evaluate((element) => element.tagName)).toBe("A");

  const downloadEvent = page.waitForEvent("download");
  await downloadLink.click();
  const download = await downloadEvent;
  expect(await download.failure()).toBeNull();
  expect(download.suggestedFilename()).toMatch(/\.csv$/i);
  const savedDownload = testInfo.outputPath(download.suggestedFilename());
  await download.saveAs(savedDownload);
  expect((await stat(savedDownload)).size).toBeGreaterThan(0);
  const downloadedCsv = await readFile(savedDownload, "utf8");
  const [header, ...downloadedRows] = downloadedCsv.trimEnd().split("\n");
  const identifiers = downloadedRows.map((row) => row.split(",", 1)[0]);
  expect(header).toBe('"Index","age","segment;;"');
  expect(downloadedRows).toHaveLength(2_000);
  expect(new Set(identifiers).size).toBe(2_000);
  expect(downloadedCsv).not.toContain("_RARE_");
  await expectNoHorizontalOverflow(page, "1280×800");
});

test("@compact generated XLSX takes the real sheet branch with keyboard and labelled state", async ({
  page,
}, testInfo) => {
  const observations = observeApi(page);
  await openBootstrappedStudio(page);

  await page.keyboard.press("Tab");
  const skipLink = page.getByRole("link", { name: /본문으로 건너뛰기/ });
  await expect(skipLink).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("main")).toBeFocused();

  const xlsxPath = testInfo.outputPath("two-sheet-fixture.xlsx");
  const xlsxSize = await writeXlsxFixture(xlsxPath);
  await page.getByLabel("CSV 또는 XLSX 파일").setInputFiles(xlsxPath);
  const uploadButton = page.getByRole("button", { name: "업로드하고 검사" });
  await uploadButton.focus();
  await page.keyboard.press("Enter");

  await expect(page.getByRole("heading", { name: "XLSX 시트 선택" })).toBeVisible({
    timeout: 120_000,
  });
  const uploadCreation = requireObservation(
    observations,
    "POST",
    "/api/v1/datasets/uploads",
    "XLSX upload creation",
  );
  await expectApiStatus(uploadCreation, 201, "XLSX upload creation");
  const upload = (await uploadCreation.response.json()) as DatasetUploadResponse;
  await assertResumableUpload(observations, upload.dataset_id, xlsxSize);
  const completion = requireObservation(
    observations,
    "POST",
    `/api/v1/datasets/${upload.dataset_id}/complete`,
    "XLSX safe preflight",
  );
  await expectApiStatus(completion, 202, "XLSX safe preflight");
  expect(((await completion.response.json()) as DatasetUploadResponse).state).toBe("sheet_required");
  await expectApiStatus(
    requireObservation(
      observations,
      "GET",
      `/api/v1/datasets/${upload.dataset_id}/sheets`,
      "XLSX sheet discovery",
    ),
    200,
    "XLSX sheet discovery",
  );

  const sheetSelect = page.getByLabel("처리할 시트");
  await expect(sheetSelect.locator("option")).toHaveCount(2);
  await sheetSelect.focus();
  await sheetSelect.selectOption("검증 데이터");
  await expect(sheetSelect).toHaveValue("검증 데이터");
  await page.keyboard.press("Tab");
  const confirmSheet = page.getByRole("button", { name: "이 시트로 계속" });
  await expect(confirmSheet).toBeFocused();
  await page.keyboard.press("Enter");

  const schemaHeading = page.getByRole("heading", { name: "열 스키마 확인" });
  await expect(schemaHeading).toBeVisible({ timeout: 120_000 });
  await expect(schemaHeading).toBeFocused();
  await expectApiStatus(
    requireObservation(
      observations,
      "PUT",
      `/api/v1/datasets/${upload.dataset_id}/sheet`,
      "selected XLSX sheet conversion",
    ),
    200,
    "selected XLSX sheet conversion",
  );
  await expectApiStatus(
    requireObservation(
      observations,
      "POST",
      `/api/v1/datasets/${upload.dataset_id}/profile`,
      "selected XLSX profile",
    ),
    202,
    "selected XLSX profile",
  );
  await expectApiStatus(
    requireObservation(
      observations,
      "GET",
      `/api/v1/datasets/${upload.dataset_id}/profile`,
      "selected XLSX profile result",
    ),
    200,
    "selected XLSX profile result",
  );
  await expect(page.getByLabel("id 유형")).toBeVisible();
  await expect(page.getByLabel("group 역할")).toBeVisible();
  await expect(page.locator(".step-rail li")).toHaveCount(6);
  await expect(page.locator('.step-rail button[aria-current="step"]')).toContainText("스키마");
  await expectNoHorizontalOverflow(page, "768×900");
});
