# Tape Data Product maintenance

Start with README.md (the full report) and docs/dataset-build.md (reproduction). Current equations and timing are in docs/tape_data_product/README.md and docs/query-contract.md. Use the installed tape_data_product package and tape-product CLI; do not introduce sibling imports or personal runtime paths.

Preserve all eighteen compact 60s/300s fields, masks, support, timestamp ordering, discovery and causal query semantics. Version intentional semantic changes. Integrity, independent reconstruction, historical reproduction, descriptive research and executable expectancy are separate claims.

Use small synthetic fixtures and fake clients for ordinary tests. Run data work sequentially. Projected batches default to 4,096 rows, at most 25,000; resident state must be bounded by batches/fixed histories. On an 8 GiB machine target at most 2 GiB process-tree RSS and stop below 3 GiB. Before a full external-data run measure the exact path on a representative session-start sample, present rows/time/RSS/transfer/disk projections and obtain confirmation. Preserve production disk reserves; never automatically rerun resource-stopped work.

R2 is durable storage; local disks are bounded caches. Canonical objects are immutable and existing identities must match exactly. Upload, deletion, bulk acquisition and publication need explicit task scope. Keep credentials, private catalogs and detailed market-data rows outside Git.

Keep docs reader-facing: usage, current methodology, design reasons and concise evidence. Implementation specs, prompts, handoffs, progress logs and obsolete proposals stay outside this repository and package. Source identities must cover all transitive material dependencies and packaged semantic metadata. Do not weaken provenance checks to remove obsolete prose.
