# Documentation

The root [research report](../README.md) is the client-facing description of the completed endpoint/EW data product, empirical population, figures, query example, and limitations.

| Document | Purpose |
|---|---|
| [Dataset build and reproduction](dataset-build.md) | Installed raw-to-base-to-feature-to-query workflow |
| [Acquisition and canonical storage](acquisition.md) | Reference screening, canonical T/Q pairs, local verification, and explicit R2 operations |
| [Feature pipeline](feature-pipeline.md) | One-second base replay and endpoint/EW feature builders |
| [Endpoint/EW contract](contracts/endpoint-ew-v1.md) | Schemas, units, masks, clocks, coverage, resets, and numerical rules |
| [Endpoint data access](endpoint-data.md) | Verified references, projected Arrow reads, DuckDB queries, and CLI access |
| [Architecture](architecture.md) | Package boundaries, identities, and bounded execution model |
| [Historical data access](data-access.md) | Inputs required to reproduce or extend the reported release |
| [Development](developer-guide.md) | Supported environment and focused verification workflow |

Private catalogs, credentials, detailed market-data rows, VM paths, operational receipts, and temporary research notes are intentionally excluded from the public repository.
