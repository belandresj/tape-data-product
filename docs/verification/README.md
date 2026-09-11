# Verification evidence

## Initial standalone verification — September 11, 2026

The original extraction ran without network access, private data, credentials, or reads from sibling source checkouts on the tested paths. These are historical synthetic measurements, not a fresh six-month dataset acceptance.

| Check | Recorded result | What it establishes |
|---|---|---|
| Regression suite | 166 passed; 15.345s monitored wall time; 188.58 MiB peak process-tree RSS | Feature/reference parity, persisted reconstruction and corruption rejection, eligibility and boundary handling, query behavior, and standalone imports on synthetic fixtures |
| Synthetic example | 720 output rows from 721 quotes and 8,640 trades; 1.304s; 120.86 MiB peak process-tree RSS | Event generation → compact feature calculation → independent reconstruction → projected query reader and state machine |
| Synthetic query result | 421 available and 299 unavailable observations; one right-censored interval with 417 active seconds | Expected behavior on the invented bounded example, not market performance |
| Environment | Python 3.13 on macOS; existing installed dependencies reused | Verification in the recorded environment; a pristine dependency install and other operating systems were not tested |

The original [test resource record](tests-resources.json), [demo resource record](demo-resources.json), and [worked transition oracle](oracle.json) are preserved unchanged. Command paths inside those records describe their original execution. Dependency versions are in [requirements-verified.txt](../../requirements-verified.txt).

The sampler checked worker and descendant RSS every 50 ms with a 768 MiB stop for these checks. No full external-data run was performed. Source integrity, independent numerical reconstruction, descriptive findings, and executable trading expectancy are separate claims.

## Documentation cleanup verification — September 11, 2026

After reorganizing documentation and updating the staged-content checker, **168 tests passed** in 16.903s monitored wall time at **212.47 MiB peak process-tree RSS**. The existing 720-second synthetic example also passed, including independent numerical reconstruction, in 2.099s at **120.92 MiB peak process-tree RSS**. Both completed without a resource stop. The [measurement record](documentation-cleanup.json) preserves the commands and exact byte measurements.

The content checker accepted the published report figures only after their staged bytes matched the publication manifest. Regression tests also rejected a modified image, an unlisted PNG, and a Markdown link to an untracked local file. Calculation source, hash-bound contracts, cohort configuration/results, and report figures were unchanged.

## Repeat bounded verification

From the repository root after following the [developer guide](../developer-guide.md), use a fresh output directory:

```sh
mkdir -p output/verification-check
.venv/bin/python -B scripts/measure.py output/verification-check/tests.json \
  .venv/bin/python -B -m pytest -q
.venv/bin/python -B scripts/measure.py output/verification-check/demo.json \
  .venv/bin/python -B scripts/offline_run.py examples/synthetic_demo.py \
  --output output/verification-check/synthetic
.venv/bin/python -B src/04_research/verify_tape_cohort_query.py --oracle
.venv/bin/python -B scripts/review_contents.py
```

The content checker inspects the Git index, so stage intended publication files before running it. It checks local links, excluded data types, common credential patterns, and the exact approved report assets. It is a heuristic publication check, not proof that every possible secret has been detected. Tests and examples use invented data; full symbol-day or corpus work still requires measured representative resource acceptance and explicit confirmation.
