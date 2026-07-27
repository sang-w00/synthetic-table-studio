# Changelog

## 0.1.0 - 2026-07-22

### Added

- Localhost-only six-step React workflow for CSV/XLSX upload, schema/rules, synthesis, progress, reports, and downloads.
- Disk-streaming ingestion and Parquet normalization with resumable uploads and atomic publication.
- Typed eight-rule compiler, deterministic transforms, full validation, and bounded residual rejection.
- Locked MOSTLY AI ARGN utility worker with deterministic bounded fit/generation and fresh-process checkpoint loading.
- Fail-closed DP boundary and privacy ledger, including a passing pinned DPMM trusted-curator checkpoint contract; the application fit/sample route remains disabled.
- Primary and optional advanced evaluation, release-safety filtering, canonical content hashes, and CSV/Parquet/XLSX exports.
- M4 sample and 2M×70 scale verification harnesses, SBOM/integrity manifests, and real-backend Playwright smoke tests.

### Changed

- Replaced setup-oriented interface copy with a direct data-to-report workflow, explicit utility/DP boundaries, and workload-specific epoch/model guidance.
- High-cardinality identifier candidates are now surfaced for confirmation, excluded columns stay out of model input, and generated identifiers are reconstructed deterministically after bounded rejection.
- Utility reports now project the runtime evaluation into requested/generated row counts, baseline-excess summaries, and per-column distance tables with plain-language interpretation.
- Reclassified the DPMM checkpoint and serialized fit RNG as non-downloadable trusted-curator state, while proving that fresh-process generation replaces it with an explicit public sampling seed.

### Verified

- Approved sample: SHA-256 `a268757667274304004d201726053d642c16b8ee5332a7045b2ae713aa7d9dd3`, 989,502 rows, 21 columns.
- Real ARGN sample path: 50,000 training rows, 5 epochs, exactly 100,000 synthetic rows with all configured rules satisfied.
- Scale control: 2,000,000×70 under a 1 GiB DuckDB limit with observed spill and equivalent Parquet/CSV decoded content hashes.

### Known limitations

- Formal DP execution is not exposed in the UI or API until the reviewed app-side MST fit/sample route is implemented; a passing engine probe alone does not enable jobs.
- No production L40S capacity result is included; that gate requires the designated NVIDIA L40S 48 GB ×4 host.
- ARF and ForestFlow are not pinned in this repository, so three-engine/three-seed non-inferiority is reported as unavailable rather than inferred.
- The web production bundle emits a non-failing warning because the main JavaScript chunk exceeds 500 kB.
