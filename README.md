# Synthetic Table Studio

대용량 CSV/XLSX를 로컬 디스크에서 스트리밍 처리하고, 규칙을 적용한 합성 데이터와 품질 보고서를 만드는 localhost 전용 웹 애플리케이션입니다. 브라우저는 원본 파일 전체를 메모리에 적재하지 않습니다.

일반 독자를 위한 기능·개인정보 보호·검증 결과 설명은 [재현자료 생성 프로그램 설명 및 개발 보고서](PROGRAM_REPORT.md)를 참고하십시오.

## 현재 보장 범위

- CSV: 재개 가능한 청크 업로드, 방언 확인, UTF-8/CP949/EUC-KR 검사, Parquet 정규화
- XLSX: 시트 선택, 단일 시트 스트리밍 변환, Excel 행/열 한도 검사
- 규칙: `not_null`, `allowed_values`, `range`, `fixed_combination`, `conditional_set`, `sum_equals`, `compare`, `mask_prefix`
- Utility 합성: 잠긴 `mostlyai-engine==2.6.2` ARGN worker, bounded fit/generate, 규칙 기반 복구·잔여 거절
- 평가: 1차 품질 보고서와 별도 eval worker의 KS/TVD·결측률, 열 쌍, C2ST, downstream, Gower/Anonymeter 경험적 개인정보 진단, CSV/Parquet 내보내기, canonical content SHA-256
- 보고서: 담당자용 HTML/JSON 보고서와 함께, 비전문가가 읽는 `쉬운 품질 보고서`를
  한글 문서(HWPX)로 발행합니다. 표준 라이브러리만으로 OWPML 패키지를 작성하며,
  공개 안전 등급은 원본 보고서에서 상속합니다
- 보안: loopback-only 기본값, Host/Origin 검사, 세션 쿠키, 경로 confinement, 원자적 publish

잠긴 `dpmm==0.1.9` MST 경로는 행 단위 add/remove 인접성으로 동작합니다. 사용자가 사전에 공개되었다고 attest한 범주·bin 메타데이터만 `/api/v1/privacy/public-metadata`로 등록한 뒤 fit을 시작하며, checkpoint와 private fit RNG는 `trusted_curator_internal`, `downloadable=false`, `release_safe=false`로 유지합니다. fresh sample worker는 공개 `sampling_seed`로 RNG 상태를 교체합니다. DP 작업은 두 보고서를 분리합니다. 담당자용 `primary_report_*`는 원본·holdout 기반 품질과 경험적 개인정보 진단을 포함해 다운로드할 수 있지만 `release_safe=false`, `contains_private_source_information=true`입니다. 외부 공개용 `dp_release_report_*`는 ledger와 공개 출력 allowlist만 포함하며 `scope=dp_release`, `release_safe=true`, `contains_private_source_information=false`입니다.

## 요구 환경

- macOS 또는 Linux
- Python 3.11/3.12와 [uv](https://docs.astral.sh/uv/)
- Node.js 22와 npm
- 로컬 디스크 workspace. 네트워크 파일시스템은 fsync/rename 보장을 별도로 검증하기 전에는 사용하지 마십시오.

## 설치

```bash
cd app && uv sync --frozen && cd ..
cd workers/argn && uv sync --frozen && cd ../..
cd workers/dpmm && uv sync --frozen && cd ../..
cd workers/eval && uv sync --frozen && cd ../..
cd web && npm ci && cd ..
./scripts/build-sbom
```

잠금 및 공급망 산출물:

- Python: 각 환경의 `uv.lock`
- Web: `web/package-lock.json`
- SBOM/integrity: `build/sbom/{dependencies,licenses,integrity}.json`
- 엔진 계약 probe: `probes/results/{argn_contract,dpmm_contract}.json`

## 실행

프로덕션형 로컬 실행:

```bash
./scripts/serve --workspace "$PWD/var/studio" --host 127.0.0.1 --port 8765
```

개발 모드(Vite + 내부 loopback API):

```bash
./scripts/serve --workspace "$PWD/var/studio" --host 127.0.0.1 --port 8765 --dev
```

브라우저에서 `http://127.0.0.1:8765`를 엽니다. 비-loopback bind는 기본 거부됩니다. `--unsafe-allow-non-loopback`은 인증·TLS·reverse proxy·Host/Origin 정책을 운영자가 별도로 책임지는 경우에만 사용하십시오.

## 워크플로

1. CSV/XLSX 업로드
2. CSV parse 옵션 또는 XLSX 시트 확인
3. schema 검색·검토 후 저장
4. 8개 규칙 중 필요한 규칙 저장·정규화
5. Utility 또는 형식적 DP 모드에서 학습·생성 실행
6. 모드별 보고서와 공개 경계를 확인한 뒤 허용된 산출물 다운로드
   (한글 문서로 된 쉬운 품질 보고서, 자세한 웹 보고서, 생성 데이터, 기술용 JSON)

원본 위반 집계, private audit, DPMM checkpoint와 fit RNG는 릴리스 불가 산출물입니다. DP 공개 경계에서는 `release_safe=true`이고 `contains_private_source_information=false`인 합성 결과와 allowlist 기반 공개 보고서만 허용합니다. source-derived domain, advanced evaluator, secret/aux 입력은 명시적 budget과 release-safety 검증 없이 다운로드할 수 없습니다.

## 검증

```bash
PYTHONPATH=app/src app/.venv/bin/python -m pytest -q \
  --ignore=tests/contract/test_advanced_evaluation.py
PYTHONPATH=workers/eval/src:app/src workers/eval/.venv/bin/python -m pytest -q \
  tests/contract/test_advanced_evaluation.py
app/.venv/bin/ruff format --check app/src tests scripts/serve scripts/verify
app/.venv/bin/ruff check app/src tests scripts/serve scripts/verify
cd web && npm run typecheck && npm run lint && npm run build && npm run test:e2e

```

Playwright는 Chromium, Firefox, WebKit에서 동일한 mock-backed six-step 흐름과 DP release 복구를 검증합니다.

실제 backend browser smoke:

```bash
cd web
STS_BASE_URL=http://127.0.0.1:8765 npx playwright test --config playwright.real.config.ts
```

M4 sample gate(승인된 sample SHA/크기/행/열을 모두 확인):

```bash
STS_SAMPLE_CSV=/absolute/path/to/sample.csv ./scripts/verify sample-m4
```

M4 scale gate(기본 2,000,000×70, 1 GiB DuckDB 한도, spill 필수):

```bash
./scripts/verify scale-m4
```

결과는 `benchmarks/results/`에 기록됩니다. 실행 기록은 머신마다 다르므로 저장소에 커밋하지 않습니다.
벤치마크 CSV가 자체 헤더를 쓰면 `benchmarks/sample-column-map.json`(정규 이름 → 원본 헤더)을 두면 됩니다.
capacity estimate는 production capacity proof가 아닙니다.

## 운영 경계와 복구

- 모든 작업 산출물은 `--workspace` 아래에만 저장됩니다.
- publish는 `.part`/staging에서 검증 후 atomic rename합니다. 재시작 시 orphan reservation과 미완료 파일을 정리합니다.
- job terminal 상태는 immutable합니다. 취소된 job은 새 job ID로만 resume합니다.
- worker stdout은 비어 있어야 하며 request/result/events JSON 파일만 프로토콜로 사용합니다.
- resource admission은 disk ceiling과 worker RSS lease를 보수적으로 예약합니다.
- L40S 48 GB ×4 production gate는 해당 host에서 별도 실행해야 합니다. M4 결과만으로 55M×70 production readiness를 선언하지 마십시오.
