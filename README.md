# Synthetic Table Studio

대용량 CSV/XLSX를 로컬 디스크에서 스트리밍 처리하고, 규칙을 적용한 합성 데이터와 품질 보고서를 만드는 localhost 전용 웹 애플리케이션입니다. 브라우저는 원본 파일 전체를 메모리에 적재하지 않습니다.

## 현재 보장 범위

- CSV: 재개 가능한 청크 업로드, 방언 확인, UTF-8/CP949/EUC-KR 검사, Parquet 정규화
- XLSX: 시트 선택, 단일 시트 스트리밍 변환, Excel 행/열 한도 검사
- 규칙: `not_null`, `allowed_values`, `range`, `fixed_combination`, `conditional_set`, `sum_equals`, `compare`, `mask_prefix`
- Utility 합성: 잠긴 `mostlyai-engine==2.6.2` ARGN worker, bounded fit/generate, 규칙 기반 복구·잔여 거절
- 평가: 1차 품질 보고서, 선택적 고급 평가, CSV/Parquet/분할 XLSX 내보내기, canonical content SHA-256
- 보안: loopback-only 기본값, Host/Origin 검사, 세션 쿠키, 경로 confinement, 원자적 publish

형식적 DP 합성은 **현재 비활성화**되어 있습니다. 잠긴 `dpmm==0.1.9`의 checkpoint가 private fit RNG 상태를 직렬화하므로 fresh-load audit를 통과하지 못했습니다. UI/API는 이를 `formal_dp_enabled=false`로 명시하고 DP job을 fail-closed로 거부합니다. 이 상태에서 생성된 결과를 DP라고 부르면 안 됩니다.

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
3. schema 검토·정규화
4. 8개 규칙 중 필요한 규칙 저장
5. Utility 모드에서 학습·생성 실행
6. 보고서 확인 후 CSV/Parquet/XLSX 다운로드

원본 위반 집계와 private audit는 릴리스 불가 산출물입니다. DP 모드에서는 source-derived domain, advanced evaluator, secret/aux 입력도 명시적 budget과 release-safety 검증 없이 다운로드할 수 없습니다.

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

결과는 `benchmarks/results/sample-m4.json`과 `benchmarks/results/scale-m4.json`에 기록됩니다. capacity estimate는 production capacity proof가 아닙니다.

## 운영 경계와 복구

- 모든 작업 산출물은 `--workspace` 아래에만 저장됩니다.
- publish는 `.part`/staging에서 검증 후 atomic rename합니다. 재시작 시 orphan reservation과 미완료 파일을 정리합니다.
- job terminal 상태는 immutable합니다. 취소된 job은 새 job ID로만 resume합니다.
- worker stdout은 비어 있어야 하며 request/result/events JSON 파일만 프로토콜로 사용합니다.
- resource admission은 disk ceiling과 worker RSS lease를 보수적으로 예약합니다.
- L40S 48 GB ×4 production gate는 해당 host에서 별도 실행해야 합니다. M4 결과만으로 55M×70 production readiness를 선언하지 마십시오.
