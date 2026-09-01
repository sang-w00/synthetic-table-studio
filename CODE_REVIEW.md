# 코드 리뷰 결과 (2026-09-01)

`app/src`, `workers/*/src`, `web/` 전체를 읽고 확인한 결과입니다. 각 항목은 실제 코드를
읽어 재현 경로를 확인한 것만 적었고, 스타일 지적이나 "리팩터링하면 좋겠다" 류는 뺐습니다.

- **이번에 고친 것**: 15건. 아래 1장.
- **남은 것**: 18건. 아래 2장. 설계 판단이 필요하거나 변경 폭이 커서 손대지 않았습니다.

검증: `pytest tests/unit tests/integration` 196건 통과, `ruff format`/`ruff check` 통과,
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

## 2. 남은 것 (손대지 않음)

### 2.1 프라이버시 · DP 경계 — 설계 판단 필요

**A. [치명적] DP 공개 보고서의 `release_count`가 상수 `1`이고, 누적 (ε, δ) 합성이 어디에서도
계산되지 않습니다.** `jobs/runtime.py`가 ledger projection에 `"release_count": 1`을 그대로
넣습니다. `release_count`·`composition()`·`release_projection()`을 구현한
`privacy/ledger.py`의 `PrivacyLedger`는 `tests/unit/test_privacy.py`에서만 쓰이고 `app/src`
어디에서도 호출되지 않습니다. 반면 `api/jobs.py`는 `dataset_manifest_sha256`으로 같은
데이터셋의 모든 실행을 하나의 privacy scope에 묶으므로 실제로는 합성됩니다. ε=1짜리 작업을
세 번 돌리면 실제 소모는 ε=3인데, 세 보고서 모두 "누적 공개 횟수는 1회"라고 외부 독자에게
말합니다. 실제 ledger 릴리스 테이블에서 값을 읽고 scope 합성을 계산하도록 배선해야 하며,
값을 못 읽으면 기본값을 쓰지 말고 실패해야 합니다.

**B. [높음] DP 경로가 빈 `StructuralCodecs(fixed_tuples={})`를 넘겨서 `fixed_combination`·
`compare` 규칙이 있는 DP 작업이 반드시 실패합니다.** `runtime.py`의 `_write_dp_batch`와
`_evaluate_dp_curator` 두 곳입니다. `reconstruct_batch`는 `codecs.tuples_for(rule)`을 무조건
호출하므로 `RULE_CONFLICT`가 나고, `compare`는 ARGN latent 델타 열을 pop 하려다 codebook
디코딩 결과에는 그 열이 없어 전 행이 무효가 됩니다. 역시 ε을 다 쓴 뒤에 터집니다. codecs를
컴파일된 규칙에서 만드는 것은 기계적이지만, latent 기반 재구성을 DP 디코딩 경로에서
건너뛰는 부분은 설계가 필요합니다.

**C. [높음] DP 작업 막바지의 취소가 `release_safe=true` 산출물을 남긴 채 작업만 CANCELLED로
만듭니다.** ledger가 이미 `RELEASED`이고 export도 published 된 뒤에 `_advance(PUBLISHING)`이
취소 파일을 보고 예외를 던지면, UI는 "취소됨"인데 `GET .../artifacts?scope=dp_release`는
여전히 공개 묶음을 돌려줍니다. release 전이 이후를 취소 불가 구간으로 두거나, terminal
CANCELLED 작업을 `dp_release` scope에서 제외해야 합니다.

**D. [중간] `DP_LEDGER_ALLOWLIST`의 대부분이 채워지지 않습니다.** 예약 시 기록하는 record에
`accountant`, `conversion`, `wheel_sha256`, `public_metadata_hashes`,
`public_target_count_provenance`, `rule_postprocessing`, `limitations`가 없습니다. 특히
`public_metadata_sha256` ≠ 허용 목록의 `public_metadata_hashes`라 메타데이터 다이제스트가
조용히 빠지고, `limitations`가 항상 비어 `privacy/ledger.py`의 `δ > 1/public_target_count`
경고가 어떤 보고서에도 도달하지 않습니다. 외부 독자가 어떤 메커니즘 빌드가 그 보장을
만들었는지 검증할 수 없습니다.

**E. [중간] `workers/dpmm`이 감사 대상인 `PrivateFitRng`를 우회합니다.** worker가
`np.random.RandomState(os.urandom(32))`를 직접 만들어서 `privacy/rng.py`의 도메인 분리
commitment와 1회 소비 핸들이 프로덕션에서는 죽은 코드입니다(`rng_policy`가 ledger에 남지
않음). 또 `resource_usage`에 `private_fit_rows`(원본 선택 행 수)를 그대로 담아 attempt
디렉터리의 JSON에 남깁니다. 그 파일에는 `contains_private_source_information` 표시가
없습니다. worker가 별도 잠금 venv에서 돌아 `sts.privacy`를 import 하지 않으므로 설계가
필요합니다.

### 2.2 상태 기계 · 동시성

**F. [높음] 데이터셋 retry가 아무도 진행시킬 수 없는 실행 상태로 되돌립니다.**
`api/datasets.py`의 `retry_dataset`은 state를 `inspecting`/`profiling`/`normalizing`으로
되돌리고 끝납니다. 그런데 그 상태들에는 `legal_actions` 매핑이 없고, `/profile`은
`RAW_READY`, `/normalize`는 `SCHEMA_READY`를 요구하므로 아무 것도 할 수 없습니다. retry가
복구가 아니라 데이터셋을 영구히 못 쓰게 만듭니다. retry가 ledger 전이 후 실제 작업을
디스패치하도록 바꿔야 합니다.

**G. [중간] repository의 읽기 메서드가 락 없이 공유 커넥션을 씁니다.** `_transaction`은
`self._lock`을 쥐지만 `get_dataset`, `get_job`, `list_artifacts`, `replay_events` 등은 같은
커넥션을 락 없이 씁니다. 단일 `sqlite3.Connection`이므로 다른 스레드의 열린 트랜잭션이
**커밋되지 않은** 쓰기를 그대로 보여줍니다. 롤백될 상태를 API가 반환할 수 있습니다. 읽기용
컨텍스트 매니저를 두거나 읽기 전용 커넥션을 분리해야 합니다(재진입 여부 확인 필요).

**H. [중간~높음] 업로드 PATCH가 본문 전체를 메모리에 담고 이벤트 루프를 막습니다.**
`await request.body()`가 64 MiB 청크 상한을 적용하기 **전에** 전체 본문을 적재하므로 4 GB
본문을 다 받은 뒤에 `UPLOAD_TOO_LARGE`를 돌려줍니다. 이어지는 SQLite·flock·fsync도 루프
스레드에서 동기 실행됩니다. `request.stream()`으로 상한 초과 즉시 중단하고
`run_in_threadpool`로 옮겨야 합니다.

**I. [낮음~중간] 상태 조회가 이벤트 전체를 재생합니다.** `GET /jobs?limit=100`이 작업마다
모든 `EventRecord`를 만들어 마지막 하나만 씁니다. 이벤트가 쌓일수록 지연과 RSS가 무한히
늘어납니다. `ORDER BY id DESC LIMIT 1` 하는 `latest_event`를 추가하면 됩니다.

### 2.3 화면

**J-0. [높음, 신규] 스키마 자동 제안이 소수 열을 `정수`로 제안하고, 그 결과가 두 단계 뒤에
터집니다.** 브라우저 실행에서 확인했습니다. `0.0`~`3.6` 값을 가진 `score` 열의 제안 유형이
`integer`였고, 사용자가 그대로 두면 스키마 저장은 통과하고 규칙 저장까지 진행된 뒤
`SCHEMA_INVALID: column 'score' has 3000 values that cannot be normalized as integer`가
무결성 규칙 화면에서 배너로 뜹니다. 화면에는 어느 단계로 돌아가야 하는지 안내가 없습니다.
프로파일 단계에서 소수점을 가진 값이 있으면 `float`/`fixed_decimal`을 제안하거나, 최소한
스키마 저장 시점에 캐스팅을 미리 검사해 그 열로 되돌려 보내야 합니다.


**J. [높음] 작업이 끝나면 사용자가 어디에 있든 보고서 단계로 강제 이동합니다.**
`connectToJob`이 현재 stage를 보지 않고 `moveTo("report")`를 호출해서, 작업 중에 스키마나
규칙을 다시 보던 사용자가 편집 도중 끌려가고 저장 안 된 편집이 보이지 않는 단계에 남습니다.
현재 stage를 ref로 잡아 `progress`에 있을 때만 이동하고, 아니면 "결과 보기" 버튼을 띄우는
쪽이 맞습니다.

**K. [높음] 스키마를 다시 저장해도 정규화 매니페스트가 무효화되지 않습니다.**
`datasetManifestSha`가 그대로 남아, 모드 단계로 바로 건너뛰어 합성을 시작하면 새
`schema_version` + 옛 매니페스트 SHA 조합이 서버에서 거부됩니다. 사용자는 이유를 알 수
없습니다. `saveSchema`에서 SHA를 비우고 단계를 되돌려야 합니다.

**L. [중간] 스키마 표가 프로파일 통계를 배열 인덱스로 결합합니다.** 복구 경로에서는 스키마와
프로파일이 서로 다른 API에서 오므로 순서·길이가 같다는 보장이 없어, 새로고침 후 다른 열의
고유값·null 수가 표시될 수 있습니다. 이름으로 `Map`을 만들어야 합니다.

**M. [중간] 업로드 청크 재시도에 백오프가 없고, 재시도 예산이 청크마다 초기화되며,
재시도하면 안 되는 오류도 재시도합니다.** 불안정한 소켓에서 사실상 무한 루프가 됩니다.

**N. [중간] 재개 가능한 업로드를 파일명+크기만으로 판단하고, 실패해도 세션을 지우지
않습니다.** 이름과 크기가 같은 다른 파일이 낡은 바이트 위에 이어 붙고 `/complete`의 SHA
검사에서 실패하는데, 재시도해도 같은 실패를 반복합니다.

**O. [중간] `loadReport` 실패 시 진행 단계에 갇힙니다.** SSE는 이미 닫혔고 취소 버튼은
비활성이라, 성공한 합성 결과에 새로고침 없이는 접근할 수 없습니다. "보고서 다시 불러오기"
버튼이 필요합니다.

**P. [중간] SSE `onerror`가 절대 포기하지 않습니다.** 백엔드가 죽어도 "복구하고 있습니다"
문구와 멈춘 퍼센트가 무한히 남습니다. 연속 실패를 세어 수동 새로고침을 제안해야 합니다.

**Q. [중간] 프로파일링·정규화 진행률이 없습니다.** 백엔드는 `GET /datasets/{id}/events`를
제공하는데 프런트가 쓰지 않아, 큰 파일에서 "형식 검사 100%" 이후 화면이 멈춘 것처럼
보입니다.

**R. [중간] `web/tests/*.spec.ts`가 현재 UI와 어긋나 있습니다.** `보고서 해설` 제목,
`다운로드` 제목, `/다운로드 가능.*외부 공개 가능/` 텍스트는 지금 `App.tsx`에 없습니다.
이번 작업 이전부터 어긋나 있던 것이고, Playwright 브라우저가 이 환경에 없어 실행 검증을 할
수 없어 손대지 않았습니다. 로컬에서 `npm run test:e2e`로 한 번 맞춰 두시길 권합니다.

### 2.4 그 밖에

- `eslint-plugin-react-hooks`가 없어서 훅 의존성 문제가 `npm run lint`에 잡히지 않습니다.
  (이번에 고친 `ReportChart`의 불안정한 의존성 배열이 그 예입니다.)
- `vite.config.ts`에 `server.proxy`가 없어 `npm run dev`만으로는 모든 API 호출이 404입니다.
- `api.ts`의 `bootstrap`이 204 같은 빈 본문을 방어하지 않아 `SESSION_REQUIRED: Unexpected end
  of JSON input`으로 보입니다.
- 진행률을 알리는 live region이 둘(`.global-status`, `.job-overview`)이라 스크린 리더가
  이벤트마다 두 번 읽습니다.
- `convert_xlsx_to_raw_parquet`의 `openpyxl` 경로에는 이번에 넣은 엔티티 가드가 적용되지
  않습니다(openpyxl 자체 파서 사용). 별도 검토가 필요합니다.
- `styles.css`의 `.page-intro`, `.session-mark`, `.site-footer`, `.site-kicker`와 `api.ts`의
  다수 export가 사용되지 않습니다.
