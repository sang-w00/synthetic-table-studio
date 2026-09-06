# 코드 리뷰 결과 (2026-09-01)

`app/src`, `workers/*/src`, `web/` 전체를 읽고 확인한 결과입니다. 각 항목은 실제 코드를
읽어 재현 경로를 확인한 것만 적었고, 스타일 지적이나 "리팩터링하면 좋겠다" 류는 뺐습니다.

- **이번에 고친 것**: 15건. 아래 1장.
- **남은 것**: 16건. 아래 2장. 설계 판단이 필요하거나 변경 폭이 커서 손대지 않았습니다.
- **이후 해결**: A(누적 예산 합성)와 D(ledger allowlist 미충족)는 2026-09-01에 구현했습니다.

검증: `pytest tests/unit tests/integration` 202건 통과, `ruff format`/`ruff check` 통과,
`tsc -b`·`eslint --max-warnings 0`·`vite build` 통과(주 번들 263.9 kB).

추가로, 실제 서버(FastAPI + 결정적 경량 어댑터 + eval worker)와 빌드된 프런트엔드를 띄우고
Chromium으로 **6단계 워크플로 전체를 실행**했습니다. 업로드 → 스키마 → 규칙 → 모드 →
진행 → 보고서까지 통과했고, 보고서 화면에서 한글 문서를 실제로 내려받아 ZIP/XML 구조를
확인했습니다. 콘솔 오류 0건, 인쇄 미디어에서 세 탭 모두 출력, 420px 폭에서 가로 스크롤
없음. `primary_report_hwpx` 아티팩트가 실제 파이프라인에서 정상 발행됩니다.

---

## 1. 이번에 고친 것

### 정확성

| # | 위치 | 문제 |
|---|---|---|
| 1 | `evaluation/primary.py` `_constant_distance` | 한쪽 열만 상수이면 실제 거리 대신 최댓값 `1.0`을 반환했습니다. holdout이 `[0,1,2,3]`, 합성이 전부 `0`이면 진짜 KS는 0.75인데 1.0으로 보고되어, 그 열이 `max_excess`와 "우선 확인할 열" 1순위를 차지했습니다. 양쪽 다 상수일 때만 단축 경로를 타도록 바꿨고, golden 테스트의 기대값도 실제 값으로 고쳤습니다. |
| 2 | `ingest/normalize.py` | 정규화가 `__sts_row_id`의 **연속성**을 요구했는데, 취입은 원본 레코드 위치를 일부러 보존하므로 `malformed="skip"`을 쓰면 항상 빈틈이 생깁니다. 즉 skip 정책이 다음 단계에서 100% 실패했습니다. 하류(`jobs/utility.py`)는 고유성만 요구하므로, 정규화도 고유성·음수 아님만 검사하도록 완화했습니다. |
| 3 | `api/jobs.py` `_validate_dp` | 공개 불가 provenance 규칙 검사가 private fit **이후**에 일어나, ε을 전부 쓰고 나서 `DP_METADATA_NOT_PUBLIC`으로 죽었습니다(ledger는 `spent_not_released`로 영구 고정). admission 단계에서 `_compiled_rules(..., mode="differential_privacy")`를 먼저 호출합니다. |
| 4 | `jobs/supervisor.py` | worker 폴링 루프에 `try/finally`가 없어, 감시 task가 취소되면 24 GiB 리스를 쥔 ARGN 프로세스가 그대로 남았습니다. `finally`에서 프로세스 트리를 종료합니다. |
| 5 | `jobs/runtime.py` `_run_dp_job` | 성공한 DP 작업이 terminal 이벤트를 내보내지 않아 브라우저 SSE 연결이 영원히 닫히지 않았습니다. utility 경로와 같은 형태로 명시적으로 emit 합니다. |
| 6 | `api/datasets.py` | `except DomainError`만 잡아서, 디스크 부족(`OSError`)이나 `KeyError` 같은 예외가 나면 데이터셋이 `profiling`/`normalizing` 상태에 갇히고 SSE도 종료 이벤트를 못 받았습니다. `except Exception`을 추가해 `WORKER_FAILED`로 실패 처리합니다. |
| 7 | `api/datasets.py` `ParseOptionsRequest` | `has_header`, `quotechar`, `escapechar`를 받아 놓고 취입에 전달하지 않아 조용히 무시했습니다. 헤더 없는 CSV를 올리면 첫 데이터 행이 열 이름이 되어 말없이 사라졌습니다. 취입이 구현한 값만 허용하도록 좁혀 큰 소리로 거부합니다. |
| 8 | `api/jobs.py`, `ingest/normalize.py` | 열린 `pq.ParquetFile` 핸들을 닫지 않았습니다. `raw_columns`는 스키마 편집마다 호출되는 경로입니다. |

### 보안 · 경계

| # | 위치 | 문제 |
|---|---|---|
| 9 | `ingest/xlsx.py` | XML 파트 4곳이 `ElementTree.iterparse`를 그대로 써서 내부 엔티티를 확장했습니다. 500바이트짜리 중첩 엔티티 8단계면 zip 크기·압축률 가드를 전부 통과한 뒤 expat 안에서 수십 GB로 부풀어 프로세스가 OOM으로 죽습니다(이 머신에서 5단계 306바이트 → 100만 자로 확인). 엔티티 선언은 DOCTYPE이 있어야만 가능하고 DOCTYPE은 프롤로그에만 올 수 있으므로, 멤버 앞부분에서 `<!DOCTYPE`/`<!ENTITY`를 발견하면 파싱 전에 거부합니다(`_open_xml`). |
| 10 | `api/security.py` | Host 허용 목록은 소문자로 정규화하는데 Origin은 아니었습니다. `STS_PUBLIC_HOST=MyLaptop.local`이면 브라우저가 보내는 소문자 Origin이 목록과 안 맞아 모든 POST/PUT/PATCH가 `ORIGIN_REJECTED`로 거부됩니다(읽기 전용 앱이 됨). 양쪽 다 소문자로 비교합니다. |
| 11 | `jobs/runtime.py` `_evaluate_dp_curator` | train/holdout 분할 키를 공개된 `job_id`에서 SHA-256으로 유도하면서 `key_commitment_sha256`으로 공표했습니다. 공개 보고서에 job id가 그대로 실리므로 누구나 키를 재계산해 특정 행의 소속 파티션을 알아낼 수 있어, 숨김도 구속도 아닙니다. utility 경로처럼 `secrets.token_bytes(32)`를 씁니다. |
| 12 | `storage/repository.py` `create_privacy_scope` | 존재 확인 `SELECT`가 트랜잭션 밖에 있어, 같은 데이터셋에 DP 작업 두 개가 동시에 들어오면 두 번째가 raw `sqlite3.IntegrityError`로 터지고 `application/problem+json`이 아닌 500이 나갔습니다. `SELECT`를 트랜잭션 안으로 옮겼습니다. |

### 성능 · 공급망

| # | 위치 | 문제 |
|---|---|---|
| 13 | `api/events.py`, `api/jobs.py` | SSE 생성기가 100 ms마다 동기 SQLite 질의를 이벤트 루프에서 실행했습니다. 탭 몇 개만 열려도 초당 수백 질의가 루프를 점유하고, 쓰기 트랜잭션이 열려 있으면 전체 서버가 그동안 아무 요청도 처리하지 못합니다. `run_in_threadpool`로 옮겼습니다. |
| 14 | `app/pyproject.toml` | `reportlab>=5.0.1`은 프로젝트에서 유일하게 핀이 없는 의존성인데 어디서도 import 하지 않습니다. 잠금 갱신 때 새 메이저가 딸려 들어와 SBOM·감사 대상만 넓힙니다. 제거하고 `uv lock`을 다시 만들었습니다(reportlab·pillow·charset-normalizer 제거, 다른 핀 변화 없음). |
| 15 | `workers/argn/pyproject.toml` | 빌드 백엔드 `hatchling`만 핀이 없었습니다. `workers/dpmm`과 같이 `hatchling==1.29.0`으로 맞췄습니다. |

### 화면(UI/UX)

- **판정을 화면에서 바로 알 수 있게** 했습니다. 탭 위에 생성 행 수 / 강제 규칙 위반 /
  분포 차이 중앙값 타일을 두고, 각 타일이 통과·주의 색을 갖습니다. 분포 차이 구간은
  "합격 기준이 아니라 읽기용 참고선"이라고 명시합니다.
- **`.report-callout.success`에 대응하는 CSS가 아예 없어서**, 앱에서 유일한 "통과" 메시지가
  경고 두 개와 똑같은 빨간 상자로 나왔습니다. accent 색으로 고쳤습니다.
- **서버가 보내는 `report.narrative`를 받아 놓고 렌더링하지 않았고**, 그것을 위한
  `.report-narrative` CSS도 죽은 코드였습니다. 이제 "결과 해석"으로 표시합니다.
- **용어 풀이**(`기준선 초과`, `KS·TVD`, `C2ST·AUROC`, `TRTR·TSTR`, `Gower`, `Anonymeter`,
  `ε·δ`)를 접이식 블록으로 넣었습니다.
- **고급 평가**가 값 없이 "계산됨 / 적용 불가"만 보여주던 것을 실제 수치와 "좋은 값은
  어떤 모양인지" 설명으로 바꿨습니다.
- **열별 거리 표**를 기준선 초과 내림차순으로 정렬하고 0.1 이상 행에 주의 색을 넣었습니다.
- **다운로드 영역**을 `쉬운 품질 보고서 / 자세한 품질 보고서 / 생성 데이터 / (접힘) 기술용
  JSON`으로 나누고, 모든 링크에 `download` 속성을 붙였습니다. 예전에는 HTML·JSON 보고서를
  누르면 탭이 SPA에서 떠나 세션 상태가 날아갔습니다.
- **인쇄**: `@media print`가 없어 인쇄 버튼이 앱 크롬을 통째로 찍고 열려 있지 않은 탭 두 개는
  빠뜨렸습니다. 탭 패널을 항상 마운트하고 `hidden`으로 전환하도록 바꾼 뒤, 인쇄 시 세 탭을
  모두 펼치고 단계 레일·상태바·다운로드를 감춥니다.
- **새 작업을 시작할 때** 이전 작업의 보고서와 다운로드 링크를 지웁니다. 이전에는 두 번째
  실행 중에 보고서 탭을 누르면 이전 결과가 현재 결과처럼 보였습니다.
- **접근성**: 탭 패널과 가로 스크롤 표에 `tabIndex={0}`(키보드로 스크롤 불가 → WCAG 2.1.1),
  차트에 범례와 `aria-describedby`로 연결된 숨김 데이터 표, `index.html`의 `lang="ko"`,
  `--color-line-strong` 대비를 2.78:1 → 3.2:1 이상으로 조정.
- **죽은 토큰**: `--color-surface-subtle`은 정의된 적이 없어 기술용 JSON 패널이 배경을
  잃고 있었습니다.

---

## 2. 두 번째 패스에서 해결한 것 (2026-09-06)

첫 리뷰에서 "설계 판단 필요"로 남겨 둔 항목을 전부 구현했다. 판단이 필요했던 곳은 보수적인
쪽을 택했고 그 이유를 각 항목에 적었다. 검증: 단위·통합 210건 + eval worker 계약 8건 통과,
`ruff`·`tsc`·`eslint`(react-hooks 규칙 활성)·`vite build` 통과, mocked Playwright 5건 통과,
실제 서버 + Chromium으로 6단계 전체와 아래 복구 시나리오를 실행.

### 프라이버시 · DP 경계

**B. DP 경로의 빈 codecs → `fixed_combination`·`compare` 규칙 항상 실패.** 해결.
`build_public_codecs(compiled)`가 공개 `allowed_tuples`만으로 codecs를 만들고(원본 추론
없음), `repair_and_validate_candidate(..., materialized=True)`가 codebook이 이미 실체화한
열을 latent에서 복원하려 들지 않고 공개 tuple에 직접 대조한다. 레거시 경로가 두 열을 모두
NULL로 만들고 전 행을 무효 처리하던 실패를 단위 테스트가 고정한다.

**C. DP 막바지 취소가 CANCELLED 작업에 release-safe 산출물을 남김.** 해결, 두 겹으로.
(1) 마지막 취소 확인은 ledger RELEASED 전이 **직전**이고, 그 이후 단계는 취소를 받지 않는다
— 예산이 공개 출력에 대해 이미 쓰였으므로 끝까지 완료하는 것이 사실을 말하는 유일한 방법.
(2) 방어 심층: `dp_release` scope 조회는 CANCELLED/FAILED 작업에 대해 빈 목록을 돌려준다.

**E. dpmm worker가 `PrivateFitRng`를 우회, `private_fit_rows` 노출.** 해결. worker는
`sts`를 import할 수 없으므로 `PrivateFitRng.take_numpy_random_state`를 바이트 단위로 그대로
미러링한다(OS CSPRNG 256비트 → 도메인 분리 SHA-256 commitment → SeedSequence → RandomState,
사용 즉시 제로화). commitment는 `rng_policy`로 fit 결과와 ledger projection·공개 allowlist에
실리고, worker가 commitment를 내지 않으면 작업이 실패한다. 원본 선택 행 수는 더 이상 보고하지
않는다.

### 상태 기계 · 동시성

**F. retry가 진행 불가 상태로 되돌림.** 해결. FAILED → 실패한 작업 **직전의 안정 상태**
(STAGED / RAW_READY / SCHEMA_READY)로 돌리고 같은 작업을 즉시 재디스패치한다.

**G. 읽기 메서드가 락 우회.** 해결. 모든 읽기가 `RLock` 아래에서 실행되어 커밋되지 않은
쓰기를 보지 않는다.

**H. 업로드 PATCH가 본문 전체를 메모리에 적재.** 해결. `Content-Length`가 한도를 넘으면
본문을 읽지 않고 413, 아니면 `request.stream()`으로 받다가 한도 초과 시 즉시 중단.
SQLite/flock/fsync 쓰기는 스레드풀로.

**I. 상태 조회가 이벤트 전체 재생.** 해결. `latest_event()`(`ORDER BY id DESC LIMIT 1`).

**K. 정규화 후 스키마 재편집 불가.** 해결. `POST /datasets/{id}/reopen`이 NORMALIZED /
SCHEMA_READY → PROFILED로 되돌리며 normalized 매니페스트를 무효화한다. 실행 중인 작업이 있으면
거부. 프런트는 편집 시 자동으로 reopen을 호출한다.

### 화면

**J-0. 소수 열이 `정수`로 제안됨 — 근본 원인 수정.** DuckDB 1.5는 `try_cast('1.2' AS
BIGINT)`를 **반올림해 1로 성공**시킨다. 프로파일러는 castability만 봤고 정규화기는
`[+-]?[0-9]+` 정규식을 요구해 둘이 어긋났다. 프로파일러가 정규화기와 **같은 판정식**을 쓰도록
고쳐 제안 유형은 이제 정규화가 실제로 받아들이는 유형이다. 브라우저에서 `score` 열이 `float`로
제안되고 기본값 그대로 정규화까지 통과함을 확인. (이전 패스의 "실패 시 스키마 단계로 자동
복귀"는 그대로 유지되어 이중 안전장치가 된다.)

**J. 작업 완료 시 강제 이동.** 해결. 진행 단계에 있을 때만 이동, 아니면 상태바에 "결과 보기".
**L. 프로파일을 인덱스로 결합.** 해결. 이름 기준 `Map`.
**M. 업로드 재시도 백오프 없음.** 해결. 지수 백오프(0.5s→8s), 총 5회, 재시도 가능 오류만.
**N. 재개 판단이 이름+크기.** 해결. 머리·꼬리 64 KiB + 크기의 fingerprint를 대조하고 실패
시 세션을 잊는다. 최종 안전장치는 여전히 서버의 `/complete` SHA-256.
**O. 보고서 로드 실패 시 갇힘.** 해결. "보고서 다시 불러오기" 버튼.
**P. SSE가 포기하지 않음.** 해결. 연속 실패 상한 후 "다시 연결" 버튼과 명확한 상태 문구.
**Q. 프로파일·정규화 진행률 없음.** 해결. 데이터셋 SSE 스트림을 구독한다.
**R. Playwright spec 낡음.** 해결. 5건 모두 현재 UI 기준으로 갱신·통과.

### 그 밖에

`eslint-plugin-react-hooks@7.1.1`(eslint 10 호환) 활성 — 즉시 실제 의존성 누락 1건을 잡아
정리했다. `vite.config.ts`에 `/api` 프록시. `bootstrap` 빈 본문 가드. 중복 live region 제거.
openpyxl 변환 경로에도 DOCTYPE·엔티티 가드(모든 XML 멤버 머리 검사). 죽은 CSS·export 정리.
`workers/dpmm`·`workers/eval`가 각자의 ruff 설정에서 lint clean.

## 3. 남은 것

이 저장소 안에서 코드로 해결할 수 없는 것들이다.

- **누적 예산의 scope 경계.** privacy scope는 `dataset_manifest_sha256`으로 묶인다. 같은
  사람이 다른 파일에도 있으면 두 실행은 합산되지 않으며, 사람 단위 실제 손실은 보고된 값보다
  클 수 있다. 업로드 간 개체 연결 정보가 없으므로 도구가 알 수 없고, 보고서 본문에 그렇게
  적혀 있다.
- **실 백엔드 Playwright(`@desktop`).** ARGN worker가 있는 장비에서만 돌 수 있어 이번 패스에서
  재검증하지 못했다. mocked 5건과 실제 서버(결정적 어댑터 + eval worker) 6단계 실행으로 대체.
- **README 기존 한계.** L40S capacity proof 없음, ARF/ForestFlow 미고정 — 첫 릴리스부터
  명시된 범위 밖.
