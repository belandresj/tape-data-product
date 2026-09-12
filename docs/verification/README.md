# Verification and acceptance

The installed local workflow passes numerical regression, input-integrity checks and a fresh-wheel offline demonstration. The historical README findings and seven published images retain their original identities; they have not been regenerated from historical data by this package.

| Executed check | Result | What it establishes |
|---|---|---|
| Integrated regression suite | **214 passed**, 27.412 s, **183.97 MiB** peak process-tree RSS | Feature/reference parity, all eighteen fields and masks, support/maturity, endpoint/query timing, corruption/completeness rejection, source/discovery provenance and report accounting |
| Final inventory-interface and identity checks | **30 passed**, 4.028 s, **142.59 MiB** | Acquisition inventory → sequential calculation → generated release index; duplicate/mixed-mode rejection, installed imports and dependency/resource identity checks. This subset ran after the inventory bridge was added. |
| Fresh installed wheel, outside checkout | CLI help and complete demo passed, **24.395 s**, **300.44 MiB** | Clean Python environment without system packages; guards reject network and all source-checkout access; paginated fake acquisition → screen → T/Q → calculator/reconstruction → release/query → numerical tables → seven verified figures |

The final demonstration generated **21,600 invented trades, 1,442 quotes and 1,440 feature endpoints** across two 720-second session-start prefixes. A third reference member supplied a terminal empty minute response and remained in the denominator. The selected query produced two censored intervals and 834 active stock-seconds; those are synthetic state-machine outputs, not market results.

The [machine-readable evidence](offline.json) records exact measurements, wheel SHA-256, implementation identity and runtime dependency versions. The audited wheel contains 79 package files matching source and no private implementation material. The full suite and final subset overlap; their counts must not be added as independent tests.

All measurements sample worker-plus-descendant RSS every 50 ms with a 768 MiB synthetic-work stop. No memory stop occurred. The initial baseline encountered the production 3 GiB disk floor on this machine; tiny transaction fixtures now explicitly fake disk capacity when testing unrelated behavior, while separate tests retain reserve rejection. Production guards were not lowered.

Executed environment: **Python 3.13.1, macOS arm64**. Installation supports the documented Python 3.13 line. [Recorded direct dependency pins](../../requirements-verified.txt) describe a tested environment; the evidence JSON supplies the resolved runtime closure. CI is configured for Python 3.13/Linux but has not run remotely as part of this local implementation.

Independent review found and closed reference-denominator, first-discovery, source-mutation and discovery-binding issues plus a duplicate CLI argument. Negative tests exercise each repaired boundary. Generated figures were visually inspected; saved numerical/export checks cover ties, zeros, eligibility, tails, normalized mass and count reconciliation. Existing published assets and original feature/query configuration hashes are unchanged. The README changed only its final reproduction-guide sentence.

## Repeat bounded checks

Follow [installation](../dataset-build.md), then use fresh output paths:

```sh
python -B scripts/measure.py output/check-tests.json python -B -m pytest -q
python -B scripts/measure.py output/check-demo.json tape-product demo --output output/check-demo
tape-product report verify --input output/check-demo/figures
```

Tests reject network and sibling-source reads. Numerical reconstruction checks the feature values against stored support; it does not independently certify vendor coverage or trading usefulness.

## Acceptance still pending

No representative external sample or full external-data acceptance was executed. Free disk was below the 3 GiB production reserve, and exact historical accepted inputs were not supplied to this workflow. No full-run time/transfer/disk estimate is inferred from synthetic measurements.

A full external run requires a measured session-start sample on the exact final production path, with rows, elapsed time, peak process-tree RSS, transfer/spill and resource projections, followed by confirmation. The separate estimator/persistence experiment awaits its stable source, tests, configuration, result schema and renderer handoff. Neither that integration nor predictive/executable expectancy is claimed.
