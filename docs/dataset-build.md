# Dataset build and reproduction

The supported product is the endpoint/EW pipeline used by the V2 report:

```text
canonical T/Q -> one-second base -> endpoint/EW features -> verified reference -> query/report
```

Use Python 3.13 and install the project as a wheel. The exact Linux dependency closure used by the project is pinned in `config/vps-python313-linux.lock`; `pyproject.toml` defines the supported runtime ranges. Run `tape-product --help` and the relevant subcommand’s `--help` for complete arguments.

## 1. Acquire or provide canonical inputs

The acquisition commands can build reference membership, minute-bar screening results, and canonical trade/quote pairs from explicit provider configuration. Existing canonical pairs may also be supplied directly when their immutable identities and source evidence are available.

```text
tape-product acquire ...
tape-product screen ...
tape-product storage inventory ...
tape-product storage verify ...
```

Acquisition and R2 transfer are optional and separate from calculation. See [acquisition and canonical storage](acquisition.md).

## 2. Admit and build one-second base measurements

Admission checks quantity representation, quote-size units, halt/continuity context, and immutable source identities. The normal path also requires terminal coverage evidence; the explicit historical exception retains unverified retrieval completeness, as described in [the admission boundary](feature-pipeline.md#admission-boundary). A blocked admission is not converted into zero activity.

```text
tape-product base admit ...
tape-product base build ...
tape-product base verify ...
```

Each completed member contains `base.parquet`, `context.json`, and `manifest.json`. Replay is streaming and uses SIP-time ordering; it does not require a full trade/quote join.

## 3. Build endpoint/EW features from base

```text
tape-product features build-from-base ...
tape-product features verify-from-base ...
```

The feature builder consumes only the completed base partition and context. It writes `features.parquet`, `support.parquet`, and a manifest bound to the base manifest. Default views use 30s/120s EW half-lives and 60s/300s exact-age p90 windows. Compatible alternative feature settings can reuse base without replaying T/Q.

For multiple members, `tape-product calculate plan` creates an immutable, hash-addressed plan; `calculate run` executes the accepted plan with explicit worker and resource limits. Completed members may be verified and reused. Interrupted members are rebuilt from session start.

## 4. Construct and query a verified reference

```text
tape-product endpoint-data full ...
tape-product endpoint-data verify ...
tape-product endpoint-data fields ...
tape-product endpoint-data inspect ...
tape-product endpoint-data query-fields ...
tape-product endpoint-data sql ...
```

References separate immutable content identity from trusted local data roots. Readers validate membership, manifests, hashes, schemas, keys, grids, and companion bindings before returning rows. DuckDB access requires inclusive date bounds and can optionally narrow members. Complete results stream to immutable Parquet exports; terminal previews are capped.

See [endpoint data access](endpoint-data.md) for Python and SQL examples.

## 5. Reproduce report artifacts

The report commands expose the retained endpoint/EW population reducers and the
V2 ECDF, joint-distribution, and worked-example renderers:

```text
tape-product report population ...
tape-product report activity-population ...
tape-product report activity-ecdf-calculate ...
tape-product report activity-ecdf-render ...
tape-product report feature-ecdf-calculate ...
tape-product report feature-ecdf-render ...
tape-product report joint ...
tape-product report joint-render ...
tape-product report gpus-cast ...
```

Numerical reduction is separate from rendering so labels and layout can be revised without rescanning feature data. Publication artifacts live under `reports/report_v2/`; private member-level contributions and machine-specific receipts remain outside Git.

The repository does not contain the private historical corpus, so a fresh clone can verify code and tracked report artifacts but cannot reproduce the empirical release without the inputs described in [historical data access](data-access.md).

The final eight-condition fast/slow comparison has a separate [reproduction guide](../scripts/research/half_life_dollar_throughput/README.md). It runs the recovered study script over stored V2 features, after producing the seven-condition baseline used for reconciliation.
