# Documentation

Start with the [full research report](../README.md) for the historical findings and limitations, then follow [dataset build and reproduction](dataset-build.md) to run the installed product.

This tracked documentation describes implemented, reviewable behavior of the codebase and data product. Durable rewrite specifications and accepted decisions live in tracked [planning/rewrite](../planning/rewrite/README.md). Private operational evidence and temporary coordination notes remain ignored.

| Document | Reader-facing purpose |
|---|---|
| [Reproduction guide](dataset-build.md) | Installation, complete offline demonstration and stage commands |
| [Acquisition and storage](acquisition.md) | Reference eligibility, screen, canonical source inputs and explicit R2 operations |
| [Endpoint/EW contract package](contracts/endpoint-ew-v1.md) | Implemented schemas, configuration, validity checks and transition/interface contract; production replay/features pending |
| [Endpoint/EW reference reader](endpoint-data.md) | Installed identity-bound pilot reference, 27-field registry and explicit session/population selection API |
| [V1 feature contract](reference/v1/feature-contract.md) | Implemented legacy equations, populations, clocks, support and zero/null rules |
| [V1 compact layout](reference/v1/compact-layout.md) | Implemented legacy physical fields, masks and validation |
| [V1 query contract](reference/v1/query-contract.md) | Implemented legacy configuration and causal entry/exit timing |
| [Architecture](architecture.md) | Package boundaries, provenance and resource design |
| [Data access](data-access.md) | Historical input identities and access requirements |
| [VPS operations](vps-operations.md) | Verified environment layout, bounded transfer harness and operational limits |
| [Development](developer-guide.md) | Regression and content-review commands |
