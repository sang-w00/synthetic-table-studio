# Changelog

## 0.2.0 - 2026-09-06

### Added

- README에 오프라인 설치·실행 절차를 추가했습니다. 미리 빌드된 Linux x86_64 번들의 내려받기
  링크와 SHA-256, 대상 장비 요구 사항(glibc 2.35 이상), 첫 실행 전 네 환경 적재 점검, 작업
  폴더·포트 환경 변수, 직접 빌드(교차 빌드 포함) 절차를 담았습니다.
- `scripts/build-offline-bundle`가 교차 빌드를 지원합니다. `TARGET_ARCH=x86_64`를 주면 aarch64 빌드
  머신에서도 대상 CPU용 인터프리터와 wheel을 실행 없이 내려받아 번들을 만들고(`TARGET_GLIBC`로
  manylinux 태그 선택), 자체 점검은 대상 장비에서 하도록 안내합니다. `SKIP_WEB_BUILD=1`로 이미
  빌드된 `web/dist`를 재사용할 수 있고, 하드링크·대소문자 구분·삭제가 제한된 공유 마운트에서도
  스테이징이 실패하지 않도록 인터프리터를 임시 디렉터리 경유로 복사하고 `__pycache__`는
  복사 단계에서 제외합니다.
- `scripts/build-offline-bundle`이 Python·Node·pip·uv·네트워크가 전혀 없는 리눅스 장비에서
  압축 해제 후 `./run.sh`만으로 실행되는 자체 포함 번들을 만듭니다. 네 환경 분리를 유지한
  채 각 `.venv/bin/python`을 번들 위치를 스스로 찾는 셸 shim으로 대체하고, 재배치 가능한
  CPython 3.11·3.12와 `uv.lock` 기준 패키지를 함께 넣습니다. x86_64에서 압축 3.1 GB,
  해제 6.1 GB로 실측했고 네트워크 네임스페이스에서 외부 연결이 완전히 차단된 상태로
  기동·서빙을 확인했습니다.
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

- 프로파일러가 소수 열을 `정수`로 제안하던 근본 원인을 고쳤습니다. DuckDB 1.5는 `'1.2'`를
  BIGINT로 캐스팅할 때 반올림해 성공시키므로 castability만 보던 제안이 정규화기의 정수 판정과
  어긋났습니다. 이제 제안이 정규화기와 같은 판정식을 사용합니다.
- 형식적 DP 경로에서 `fixed_combination`·`compare` 규칙이 있는 작업이 ε을 다 쓴 뒤 항상
  실패하던 문제를 고쳤습니다. 공개 `allowed_tuples`만으로 codecs를 만들고, codebook이 실체화한
  열은 latent 복원 없이 공개 tuple에 직접 대조합니다.
- DP 작업 막바지의 취소가 CANCELLED 작업에 release-safe 산출물을 남기던 경합을 막았습니다.
  ledger RELEASED 전이 이후 단계는 취소를 받지 않고, `dp_release` scope 조회는 실패·취소
  작업에 빈 목록을 돌려줍니다.
- dpmm worker가 감사 대상 RNG 정책을 우회하고 원본 선택 행 수를 노출하던 문제를 고쳤습니다.
  worker는 `PrivateFitRng` 도출을 그대로 미러링해 commitment만 보고하며, 이 commitment가
  ledger와 공개 보고서까지 전달됩니다. worker가 commitment를 내지 않으면 작업이 실패합니다.
- 데이터셋 retry가 진행 불가 상태로 되돌리던 문제를 고쳤습니다. 실패 직전 안정 상태로
  돌아가 같은 작업을 즉시 재실행합니다.
- 정규화된 데이터셋의 스키마·규칙을 다시 편집할 수 있습니다(`POST /datasets/{id}/reopen`).
  실행 중인 작업이 있으면 거부합니다.
- 업로드 PATCH가 크기 검사 전에 본문 전체를 메모리에 올리던 문제를 고쳤습니다. 한도를 넘는
  순간 중단하고, 디스크 쓰기는 이벤트 루프 밖에서 합니다.
- repository 읽기가 락을 우회해 커밋되지 않은 쓰기를 볼 수 있던 문제, 상태 조회가 이벤트
  전체를 재생하던 문제를 고쳤습니다.
- 화면: 작업 완료 시 강제 이동 대신 "결과 보기"; 프로파일을 이름으로 결합; 업로드 재시도
  지수 백오프와 fingerprint 기반 재개; 보고서 재로드·SSE 재연결 버튼; 프로파일·정규화 진행률
  표시; 중복 live region 제거; `vite` 개발 서버 `/api` 프록시.
- openpyxl 변환 경로에도 DOCTYPE·엔티티 선언 가드를 적용합니다.
- Playwright spec 5건을 현재 UI에 맞춰 갱신했고, `eslint-plugin-react-hooks`를 활성화했습니다.
- 열 유형을 잘못 지정해 정규화가 실패하면 데이터셋이 `failed`로 끝나 버려 되돌릴 수 없었고,
  사용자가 다시 시도하면 `INVALID_STATE: rules can only be saved after schema validation and
  before normalization`이라는 원인과 무관한 오류만 보였습니다. 이제 캐스팅 실패는 사용자가
  고칠 수 있는 입력 오류로 취급해 데이터셋을 스키마 단계(`profiled`)로 되돌리고, 문제가 된
  열 이름과 해야 할 일을 오류 메시지에 담습니다. 화면도 스키마 단계로 돌아가 해당 열만
  필터링해 보여주므로, 유형을 고쳐 저장하면 그대로 정규화까지 이어집니다. 디스크 부족 같은
  실행 오류는 예전처럼 `failed`로 남아 재시도 대상입니다.
- DP 공개 보고서가 누적 개인정보 예산을 실제로 계산합니다. 이전에는 `release_count`가 상수
  `1`이라, 같은 원본으로 ε=1짜리 작업을 세 번 돌려도 모든 보고서가 "누적 공개 1회"라고
  말했습니다. 이제 privacy scope 안에서 원본을 건드린 모든 실행(공개하지 않은 것 포함)을
  basic sequential composition으로 합산해 누적 ε·δ, 예산 사용 실행 수, 공개 횟수를 보고서와
  한글 문서에 싣습니다. ledger를 읽지 못하면 기본값을 쓰지 않고 작업이 실패합니다. 합산은
  같은 원본 파일로 인식된 실행만 포함한다는 한계도 보고서 본문에 명시합니다.
- DP release allowlist가 비어 있던 항목(`accountant`, `conversion`, `wheel_sha256`,
  `lock_sha256`, `public_metadata_hashes`, `public_target_count_provenance`,
  `rule_postprocessing`, `limitations`)을 예약 시점에 기록합니다. 메커니즘 신원은 검증된
  probe 결과에서 읽고, probe에 없는 값은 추측하지 않고 생략합니다. 외부 독자가 어떤 wheel과
  어떤 ε·δ → zCDP ρ 변환이 그 보장을 만들었는지 확인할 수 있습니다.
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

- CSV/XLSX 업로드, 스키마·규칙, 합성, 진행 상황, 보고서, 다운로드로 이어지는 localhost 전용 6단계 React 워크플로.
- 재개 가능한 업로드와 원자적 publish를 갖춘 디스크 스트리밍 취입과 Parquet 정규화.
- 8종 규칙의 타입 검사 컴파일러, 결정적 변환, 전수 검증, 상한이 있는 잔여 거절.
- 잠긴 MOSTLY AI ARGN utility worker. 결정적이고 상한이 있는 fit·generate와 새 프로세스에서의 checkpoint 적재를 사용합니다.
- ledger로 예산을 예약하는 DPMM MST fit·sample 경로. 공개 메타데이터 admission, 새 프로세스 sampling, 공개 산출물 allowlist를 적용합니다.
- 1차 평가와 분리된 고급 평가, 공개 안전성 필터링, canonical content 해시, CSV/Parquet 내보내기.
- 표본 및 200만 행×70열 규모 검증 harness, SBOM·integrity manifest, 실제 백엔드 Playwright smoke 테스트.

### Changed

- 설정 위주였던 화면 문구를 자료에서 보고서까지 곧바로 이어지는 흐름으로 바꾸고, utility와 DP의 경계, 작업별 epoch·모델 안내를 명시했습니다.
- 고유값이 많은 식별자 후보를 확인받도록 드러내고, 제외한 열은 모델 입력에서 빠지며, 생성된 식별자는 상한이 있는 거절 이후 결정적으로 재구성합니다.
- Utility 보고서와 담당자용 DP 보고서는 생성 행, 규칙 검증, KS/TVD·결측률, 열 쌍, C2ST, downstream utility, Gower/Anonymeter 경험적 개인정보 진단을 한국어 자연어로 먼저 설명하고 기계 판독 지표를 부록으로 유지합니다. DP 외부 공개 보고서는 별도 allowlist 산출물로 유지합니다.
- DPMM checkpoint와 직렬화된 fit RNG를 다운로드 불가한 trusted-curator 상태로 재분류하고, 새 프로세스 생성이 이를 공개 `sampling_seed`로 대체함을 확인했습니다.
- 스키마 검색·검토 필터, 모드에 따른 DP 공개 보고서, 접근성 있는 진행 상황 안내, Chromium·Firefox·WebKit 워크플로 검증을 추가했습니다.
- ECharts를 보고서 화면에서만 내려받는 지연 chunk로 분리해 기본 production bundle을 500 kB 아래로 줄였습니다.

### 검증

- 승인된 원본 표본으로 취입·정규화·합성·평가 전 구간을 검증했습니다.
- 실제 ARGN 표본 경로: 학습 50,000행, 5 epoch, 정확히 100,000행 생성, 설정한 규칙 전부 충족.
- 규모 통제: DuckDB 1 GiB 제한에서 200만 행×70열, spill 발생 확인, Parquet과 CSV의 디코딩 후 content 해시 일치.

### 알려진 한계

- production L40S 처리 능력 결과는 포함되어 있지 않습니다. 해당 검증에는 지정된 NVIDIA L40S 48 GB 4장 장비가 필요합니다.
- ARF와 ForestFlow는 이 저장소에 고정되어 있지 않으므로, 3개 엔진·3개 seed 비열등성은 추정하지 않고 확인 불가로 보고합니다.
