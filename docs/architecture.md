# Architecture and source map

The maintained core turns normalized SIP trades and NBBO quotes into one-second observations containing eighteen trailing 60s/300s measurements and their quality context. Start with the [dataset build guide](dataset-build.md) for the complete workflow and its current gaps.

| Layer | Main files | Responsibility |
|---|---|---|
| Source events | [Market-state decoder](../src/02_preprocessing/build_market_state.py) | Quote validity, SIP ordering, trade eligibility, and continuity semantics |
| Feature calculation | [Direct calculator](../src/03_features/direct_frozen_product.py) | Stream quote/trade inputs into bounded rolling feature and support rows |
| Product output | [Compact writer](../src/04_research/compact_product.py), [schema](../src/04_research/compact_product_schema.py) | Write features, support measurements, identities, and completion evidence; validate structure and optionally reconstruct numerical values |
| Build orchestration | [Direct runner](../src/04_research/run_direct_frozen_product.py), [inventory bridge](../src/04_research/compact_product_inventory.py), [runtime](../src/04_research/compact_product_runtime.py) | Consume prepared selection inventories, route existing or raw inputs, supervise workers, and record completion |
| Storage | [R2 helpers](../src/01_data/r2_tq_storage.py), [compact storage](../src/04_research/compact_product_storage.py) | Verified bounded staging and immutable publication when explicitly requested |
| Release inventory | [Release inventory](../src/04_research/report_release_inventory.py) | Capture/freeze accepted published membership from supplied controls |
| Historical queries | [Cohort runner](../src/04_research/run_tape_cohort_query.py), `tape_cohort_*.py` | Read compact features and apply explicit entry/continuation rules; not validated trading signals |
| Independent checks | [Reconstruction](../src/04_research/all_feature_month_verify.py), [tests](../tests/test_direct_frozen_product.py) | Check numerical reconstruction, reference parity, and boundary behavior |

Numbered source directories and historical module names are retained to preserve imports and implementation identities. Older rolling/economic reducers and all-feature, July, preview, snapshot, and inventory helpers are transitive calculation, reader, provenance, or regression dependencies. Their presence does not mean their historical methodology is part of the current feature contract.

## Specifications that are code dependencies

The following older specifications are read and hashed by retained code:

- [Rolling tape V2](rolling_tape/rolling_tape_state_v2_feature_spec.md): read by `build_rolling_tape_state_v2.py`.
- [Economic tape V3](tape_characterization_v3/tape_characterization_v3_model.md) and its [pilot specification](tape_characterization_v3/pilot_implementation_spec.md): included with the rolling specification in `economic_tape_state_v3.py` provenance.

All three are also included in the cohort pipeline’s implementation identity. Keep these paths intact until a separately tested identity migration replaces that dependency. They are supporting historical specifications, not alternative definitions of the current [feature contract](tape_data_product/README.md).

The [initial extraction provenance](provenance/README.md) records how this code was selected. Current numerical behavior must be assessed against the code and tests, not inferred from the extraction inventory.
