# Architecture and design

The installed `tape_data_product` package connects historical reference membership and minute screening to canonical SIP trade/NBBO quote pairs, compact features, verified releases, causal queries and report analysis. The [reproduction guide](dataset-build.md) is the supported workflow; the root README remains the research report.

| Package | Responsibility |
|---|---|
| `acquisition` | Explicit provider requests, reference eligibility, consecutive-minute screening and bounded event normalization |
| `storage` | Canonical pair inventories, exact object identities, local verification and explicit R2 transfer |
| `features` | Event populations, one-second measurements, rolling estimates, compact schemas, integrity and independent reconstruction |
| `query` | Expected release membership, verified projected reads and causal interval state |
| `experiments` | Versioned descriptive threshold-query comparisons |
| `analysis` | Horizon-specific report populations, distributions, numerical intermediates and offline figures |
| `stages` | Effective settings, installed source/runtime identity and immutable output receipts |
| `cli`, `demo` | Stage dispatch and a connected invented workflow |

Parquet files and indexed catalogs hold the research dataset. Massive.com supplies the market observations; the author’s private Cloudflare R2 bucket retains acquired files as durable object storage. R2 access is optional: the supported calculation and report workflow operates on explicitly supplied local files.

Storage and computation are separate. This design retains historical inputs without keeping a compute server running, while allowing verified files to be reused across research runs. The tradeoff is transfer time and local disk management: R2 storage alone does not execute the Python calculations or provide an interactive query service. The supported transfer path stages complete named pairs and verifies their hashes before local processing. Repeated broad scans can therefore be limited by staging and cache capacity.

## Semantics and provenance

Each second-ending row summarizes `[t−1s,t)`; exact-endpoint events enter the following row. The eighteen features retain their [equations and support rules](reference/v1/feature-contract.md). The query receives every endpoint, including economic failures and unavailable measurements; removing those rows before state processing would corrupt confirmation and exits.

Calculation integrity and numerical reconstruction are distinct checks. Integrity binds byte lengths, SHA-256, schemas, row counts and completion metadata. Reconstruction derives the feature values from stored one-second support and compares values, native nulls and masks. Neither establishes predictive power or executable expectancy.

The compact calculator and independent reducers descend from research source based on commit `997b28063d9fa01fcff4e978b7acb013ca18255a`; that source tree was not necessarily clean, so the commit alone is not complete provenance. The installed package contains the numerical engines and reference reducers required by regression tests. Historical module names remain where they identify an algorithm or validation lineage; the supported entry points are the domain APIs and CLI. Packaged semantic metadata preserves historical definition digests, while current source files, required resources and runtime dependencies receive new implementation identities. Existing report images and manifests retain their historical attribution and are never relabeled as output from the installed package.

`stage.json` records the stage schema, explicit inputs, effective parameters, all declared output identities, validation evidence and a transitive installed-code/runtime identity, including the installed runtime dependency closure. Large membership and observation tables remain separate on disk. Domain manifests enforce additional completeness and schema checks. A changed material dependency requires a new run identity, even if numerical results are unchanged.

## Resource bounds

Let U be reference/minute records, E trade-plus-quote events, N output seconds, F the fixed feature count, H≤300 the rolling horizon, K query conditions and C release members.

| Stage | Time / disk growth | Resident state |
|---|---|---|
| Acquisition/screen | O(U) in order; external ordering O(U log U) | Capped response pages/bytes, a batch and indexed on-disk membership |
| Event normalization/storage | O(E + bytes), sorting O(E log E) | Bounded sort buffers/spill, projected batches and transfer blocks |
| Feature calculation | O(E + N·F + N·H), O(N) output | Input/output batches, fixed rolling histories and Arrow/compression buffers |
| Reconstruction/query | O(N log H) / O(N·K) | Fixed histories/state, projected batches and bounded result writers |
| Release reconciliation | O(C log C), O(C) disk | Indexed membership controls |
| Report aggregation | Histograms O(N·F); exact ECDF ordering O(F·N log N) | Fixed histograms, capped database sort/spill and bounded fetches |
| Rendering | Bounded display points/bins | One figure canvas and reduced display representation at a time |

Projected reads default to 4,096 rows and never exceed 25,000; the calculator's output cap remains 12,288 rows. Data work runs sequentially on the 8 GiB development machine. The monitoring script samples worker and descendant RSS every 50 ms and terminates its owned tree at 768 MiB for ordinary synthetic verification. Production work targets ≤2 GiB RSS and must stop below 3 GiB, with headroom for sampling delay.

Before full external-data acceptance, run the exact production path on a representative session-start prefix, measure rows/time/RSS/transfer/spill, project the full run and obtain confirmation. A mid-session slice without prior state does not reproduce mature rolling history. Passing unit tests supplies no full-data resource acceptance.

Report extraction additionally caps member metadata at 10,000 members and each member writer at 1,024 row groups. Separate member writers prevent Arrow footer metadata from growing with corpus rows; the ordinary 4,096-row batch needs at most 15 groups for a full session.
