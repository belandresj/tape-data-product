# Tape Data Product maintenance

Start with README.md and docs/README.md. For build/reproduction work, read docs/dataset-build.md and docs/architecture.md; for feature changes, read docs/tape_data_product/README.md and docs/compact-layout.md; for query changes, read docs/query-contract.md. Setup and bounded verification commands are in docs/developer-guide.md and docs/verification/README.md. The maintained product is compact 60s/300s direction-neutral tape measurement with an existing exploratory cohort query. Do not claim that the full acquisition-to-report workflow is bundled. Older RTH V1, episode-local MU/X/Q and clustering are separate methodologies, even where lineage dependencies are retained.

Preserve feature equations, event populations, clocks, support/maturity/null rules and query timing. Any semantic change requires explicit versioning and corresponding tests. Numerical reconstruction, source integrity, descriptive findings and executable expectancy are distinct claims.

Do not import or read a sibling checkout. New plans must bind this checkout's actual transitive implementation. Never resume an original-workspace job here or mutate its artifacts.

Use synthetic fixtures for ordinary tests, no network. Data/credentials/private catalogs stay outside Git. R2 publication, deletion and removal of local data require explicit task scope. Existing immutable objects must match exact identity and must never be overwritten.

On an 8 GiB machine, normally target <=2 GiB resident memory and stop before 3 GiB. Query-specific limits can be stricter. Batch projected reads at <=25,000 rows (4,096 default); keep state bounded by batches/fixed rolling windows rather than event count. Single-process execution is preferred. Specifications must state time/memory complexity, largest resident objects, batching and measured peak RSS acceptance. Do not retain full raw Arrow and Pandas copies.

Before a full symbol-day, corpus or external-drive run, measure the exact production path on a bounded representative sample in a separately monitored process, including descendant RSS. Present sample rows/time/peak RSS and projected resources; obtain explicit user confirmation before full external-data acceptance. Synthetic tests alone do not meet this checkpoint. Do not automatically rerun resource-stopped jobs.

Keep user explanations technically precise: lead with actual behavior/results, strongest evidence, limitations and the next decision. A local commit is not a remote backup. Do not create/push a remote or make it public without the requested approval.
