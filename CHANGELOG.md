# Changelog

## Unreleased

### Added

- 모든 성공한 작업이 비전문가용 `쉬운 품질 보고서`를 한글 문서(HWPX)로 함께 발행합니다.
  결론과 구조·분포 판정, 핵심 지표 표, 먼저 확인할 열, 개인정보 보호 설명, 해석 주의사항,
  용어 해설을 담습니다. 표준 라이브러리만으로 OWPML 패키지를 직접 작성하므로 새 의존성이
  없고, 같은 입력에서 항상 같은 바이트를 만들어 아티팩트 해시가 재현됩니다. 문서의 공개
  안전 등급은 원본 보고서에서 그대로 상속하므로 DP 공개 보고서에서 만들어진 문서만
  `release_safe=true`이며, 그 문서에는 열별 원본 비교가 들어가지 않습니다.
- 보고서 화면에 판정 요약(생성 행 수 · 강제 규칙 위반 · 분포 차이)과 용어 풀이가 생겼고,
  다운로드 영역을 쉬운 보고서 / 자세한 보고서 / 생성 데이터로 나눴습니다. 인쇄하면 세 개
  탭과 접혀 있던 설명이 모두 한 문서로 나옵니다.
- 열별 거리 표의 지표 이름을 내부 식별자(`ks_distance`) 대신 읽을 수 있는 이름
  (`KS · 분포 차이`)으로 표시합니다.
- 서버 자연어 해설은 접이식으로 바꿔 "한눈에 보는 결론"과 같은 문장을 두 번 읽지 않게
  했고, 요약 지표 목록에서는 판정 타일이 이미 보여주는 값을 뺐습니다.
- 페이지마다 나던 favicon 404를 없앴습니다.

### Fixed

- 한쪽 열만 상수인 경우 KS·TVD 거리를 실제 값 대신 최댓값 1.0으로 보고하던 문제를
  고쳤습니다. 해당 열이 baseline-excess 집계와 "우선 확인할 열"을 부당하게 지배했습니다.
- `malformed="skip"`으로 취입한 자료가 정규화 단계에서 항상 거부되던 문제를 고쳤습니다.
  취입은 원본 레코드 위치를 보존하므로 `__sts_row_id`에 빈틈이 생기며, 정규화는 이제
  연속성 대신 고유성과 음수 아님만 요구합니다.
- 공개 불가능한 provenance를 가진 규칙이 있으면 DP 예산을 쓰기 전에 admission에서
  거부합니다. 이전에는 private fit이 ε을 모두 소모한 뒤에 실패했습니다.
- XLSX preflight의 XML 파트에서 DOCTYPE·엔티티 선언을 거부합니다. 수백 바이트짜리 중첩
  엔티티가 압축률·크기 제한을 모두 통과한 뒤 메모리를 고갈시킬 수 있었습니다.
- worker를 감시하던 task가 취소되어도 worker 프로세스를 반드시 종료합니다.
- 성공한 DP 작업이 terminal 이벤트를 내보내지 않아 SSE 스트림이 닫히지 않던 문제를
  고쳤습니다.
- DP 담당자 평가의 train/holdout 분할 키를 공개된 job id에서 유도하지 않고 난수로
  생성합니다. 이전 키는 공개 보고서만으로 재계산할 수 있어 commitment가 아니었습니다.
- 데이터셋 처리 중 DomainError가 아닌 예외가 나면 데이터셋이 실행 상태에 영원히 갇히던
  문제를 고쳤습니다.
- SSE 스트림이 100 ms마다 이벤트 루프에서 동기 SQLite 질의를 실행하던 문제를 고쳤습니다.
- Origin 허용 목록을 Host와 같이 소문자로 정규화합니다. 대소문자가 섞인 `STS_PUBLIC_HOST`
  에서 모든 변경 요청이 거부됐습니다.
- 같은 데이터셋에 대한 동시 DP 작업이 privacy scope 생성에서 raw IntegrityError로 500을
  내던 경합을 트랜잭션 안으로 옮겼습니다.
- CSV parse 옵션의 `has_header`, `quotechar`, `escapechar`를 받아서 조용히 버리는 대신
  지원하지 않는 값을 거부합니다. 헤더 없는 CSV가 첫 데이터 행을 말없이 잃었습니다.
- 열려 있던 `pq.ParquetFile` 핸들을 닫습니다.

### Changed

- 최종 HTML/JSON 보고서는 한눈에 보는 결론, 재현 품질, 프라이버시 보호,
  해석 제한을 자연어로 먼저 설명합니다. 담당자용 보고서는 행·규칙 검증,
  baseline-excess, 열 관계, C2ST, downstream 평가, Gower/Anonymeter 결과를
  구체적인 수치와 함께 설명하고, DP 공개 보고서는 ε·δ와 누적 공개의 의미를
  안전 비율로 오해하지 않도록 설명합니다.
- 실행 시 현재 CPU, 사용 가능 메모리, 디스크, Apple MPS 또는 NVIDIA CUDA
  장치를 감지해 worker lease, 학습 행 상한, DuckDB 메모리, 동시 작업 수와
  권장 장치를 자동 설정합니다. 작업 admission은 예상 산출물과 현재 디스크
  여유를 비교하고, ARGN 프로세스 트리 RSS도 같은 lease로 감시합니다.

## 0.1.0 - 2026-07-22

### Added

- Localhost-only six-step React workflow for CSV/XLSX upload, schema/rules, synthesis, progress, reports, and downloads.
- Disk-streaming ingestion and Parquet normalization with resumable uploads and atomic publication.
- Typed eight-rule compiler, deterministic transforms, full validation, and bounded residual rejection.
- Locked MOSTLY AI ARGN utility worker with deterministic bounded fit/generation and fresh-process checkpoint loading.
- Ledger-reserved DPMM MST fit/sample application path with public metadata admission, fresh-process sampling, and release-only artifact allowlisting.
- Primary and isolated advanced evaluation, release-safety filtering, canonical content hashes, and CSV/Parquet exports.
- M4 sample and 2M×70 scale verification harnesses, SBOM/integrity manifests, and real-backend Playwright smoke tests.

### Changed

- Replaced setup-oriented interface copy with a direct data-to-report workflow, explicit utility/DP boundaries, and workload-specific epoch/model guidance.
- High-cardinality identifier candidates are now surfaced for confirmation, excluded columns stay out of model input, and generated identifiers are reconstructed deterministically after bounded rejection.
- Utility 보고서와 담당자용 DP 보고서는 생성 행, 규칙 검증, KS/TVD·결측률, 열 쌍, C2ST, downstream utility, Gower/Anonymeter 경험적 개인정보 진단을 한국어 자연어로 먼저 설명하고 기계 판독 지표를 부록으로 유지합니다. DP 외부 공개 보고서는 별도 allowlist 산출물로 유지합니다.
- Reclassified the DPMM checkpoint and serialized fit RNG as non-downloadable trusted-curator state, while proving that fresh-process generation replaces it with an explicit public sampling seed.
- Added schema search/review filtering, mode-aware DP release reports, accessible live progress text, and Chromium/Firefox/WebKit workflow coverage.
- Split ECharts into a lazy report-only chunk, reducing the main production bundle below 500 kB.

### Verified

- Approved sample: SHA-256 `a268757667274304004d201726053d642c16b8ee5332a7045b2ae713aa7d9dd3`, 989,502 rows, 21 columns.
- Real ARGN sample path: 50,000 training rows, 5 epochs, exactly 100,000 synthetic rows with all configured rules satisfied.
- Scale control: 2,000,000×70 under a 1 GiB DuckDB limit with observed spill and equivalent Parquet/CSV decoded content hashes.

### Known limitations

- No production L40S capacity result is included; that gate requires the designated NVIDIA L40S 48 GB ×4 host.
- ARF and ForestFlow are not pinned in this repository, so three-engine/three-seed non-inferiority is reported as unavailable rather than inferred.
