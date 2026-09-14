# Historical inputs and access

Massive.com is the market-data provider used for the historical study. Cloudflare R2 was used to retain the acquired files; it is a storage service, not the source of the market observations. The complete offline demonstration uses invented inputs and needs neither a vendor account nor R2 access. Historical reproduction additionally requires authorized vendor data, immutable source identities, selection denominators and halt/continuity context. This repository does not contain private catalogs, credentials or detailed market-data rows.

The report's accepted historical release identity is
`c9abea6e3faea62e2471e8b8b883494c612cc4d48b4e44e82d9a7b47dd9aae50`.
It records 6,222 selected/completed symbol-days on 122 dates, March 9–August 31, 2026. Preserve this identity when assessing the imported report. A newly built compatible release has its own identity and is not automatically a reproduction of those results.

To reproduce the historical corpus, obtain the exact release controls (`manifest.json`, `accepted.jsonl`, `reconciliation.jsonl`, `capture_exclusions.jsonl` and referenced captured manifests), accepted calculation-identity allowlist including the recorded OCG recovery identity, and the exact source objects. Report aggregation also requires daily reference/screen denominator records and the original numerical selection. The retrospective GPUS/CAST illustration requires June 18 T/Q and feature partitions with preceding history; see the [figure lineage](../reports/report-lineage.md).

For a local build, the [reproduction guide](dataset-build.md) accepts explicit canonical pairs, discovery and halt context, builds partitions, and reconciles expected membership into a release. The query can process a compatible verified release without hardcoding the historical hash. An explicit expected-release constraint remains available for exact replay. Missing members must fail completion, including dates with zero query matches.

The original research files are retained in the author’s private Cloudflare R2 bucket, `massive-equities`. Access to that bucket is not provided or assumed. Users can acquire data with their own Massive.com credentials and run the workflow using local files, or explicitly configure their own R2 endpoint, bucket and credentials. The example account endpoint is a placeholder; the example/default bucket name does not grant access to the author’s account. Only explicit storage transfer commands connect to R2.

Reacquiring the same dates from the same provider creates a new input snapshot; it does not establish exact equality with the original study. Exact replay requires the original input identities and supporting records described above.

Canonical pairs use `tq/session_date=YYYY-MM-DD/symbol=SYMBOL/{trades,quotes}.parquet`. Existing immutable objects must match exact SHA-256, byte length, row count and source/coverage metadata; no overwrite is allowed. Raw JSONL exports require a coverage audit and explicit normalization before canonical use. See [acquisition and storage](acquisition.md) for supported input and credential configuration.

No bulk acquisition, R2 upload/deletion, market-data deletion or external acceptance is implied by installation. Read-only staging still needs the configured disk reserve. Before a full symbol-day or corpus check, measure the exact production path on a representative session-start prefix and present input rows, elapsed time, process-tree RSS, disk/spill and transfer projections for confirmation. Synthetic verification and old research approvals do not replace that checkpoint or establish redistribution rights.
