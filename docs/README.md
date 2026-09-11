# Documentation

Start with the [project report](../README.md) for dataset coverage, feature definitions, and descriptive findings.

| Document | Purpose |
|---|---|
| [Dataset build and reproduction](dataset-build.md) | What can be rebuilt now, the actual calculation path, and missing acquisition/report stages |
| [Developer guide](developer-guide.md) | Installation, synthetic example, and development commands |
| [Feature contract](tape_data_product/README.md) | Exact 60s/300s definitions, timing, and eligibility rules |
| [Compact layout](compact-layout.md) | Stored fields, masks, validation, and complexity |
| [Architecture](architecture.md) | Source-code responsibilities and why older dependencies remain |
| [Data access](data-access.md) | Private inputs and the existing historical feature-retrieval workflow |
| [Query contract](query-contract.md) | Existing exploratory cohort mechanics; no claim of validated retrieval usefulness |
| [Technical supplement](../reports/technical-supplement.md) | Report accounting and figure-source verification |
| [Verification](verification/README.md) | Dated synthetic evidence and repeatable bounded checks |
| [Provenance](provenance/README.md) | Initial source extraction and later report-asset identities |

The `rolling_tape/` and `tape_characterization_v3/` documents are hash-bound supporting specifications used by retained code. They are not the current product contract; see the architecture document before moving or editing them.
