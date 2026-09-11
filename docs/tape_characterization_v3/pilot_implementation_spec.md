# V3 economic comparison pilot implementation

The pilot is `economic_tape_state_v3_pilot_1`, comparing 2026-09-02 with
2026-09-01. Implementation and bounded sample verification precede full-data
acceptance. Legacy V2 outputs are not changed or promoted by this pilot.

## Measurement contract

The 18 fields, six families, all-coordinate eligibility gate, log1p units,
September 1-only linear median/IQR scaler and equal-family Euclidean distance
are defined in the V3 model. Bid and ask remain separate. No directional,
effective-spread, tick-size, ratio, or path-shape coordinate enters distance.

V3 corrects quoted spread at both horizons under
`joint_valid_unlocked_duration_v3_1`: integrate `spread_bps` times duration
only while the price/spread state is valid AND not locked; divide by duration
under that identical joint predicate. Do not subtract marginal locked duration
from marginal valid duration. Explicitly locked positive spreads and invalid
numerical locks contribute neither mass nor unlocked-valid duration. A zero
duration contributes zero mass, never null times zero. Trust coverage is 90%;
a complete same-generation wall window with positive duration publishes its
estimate below that threshold, but is not comparison eligible.

Durations accumulate as integer nanoseconds, not sums of floating fractions.
Rolling duration totals remain exact integers representable in float64 at
these horizons. Spread coverage gates compare `10 * duration_ns >= 9 * H * 1e9`;
depth gates compare `5 * duration_ns >= 4 * H * 1e9`. Nine 100 ms valid segments
must pass exactly 90%, and eight must pass exactly 80%, without tolerance-based
admission of observations below the threshold.

The V3 artifact metadata versions its 60s field separately from legacy V2;
there is no claim of exact aliasing to V2's incorrect marginal subtraction.

Raw SIP events use `[t-1s,t)` source intervals. The quote initializer may use
the last accepted quote from `[03:55,04:00)`; this seeds signature and quote age,
but contributes no event count or feature history. Trade initialization starts
at 04:00. The canonical quote semantics and eligible trade condition/correction,
fractional-share and latency rules are reused from `build_market_state.py`.
Keys are strictly increasing `(sip_timestamp, sequence_number)` per stream;
duplicates, unknown codes and bad ordering fail the build. Canonical numerical
invalid states remain observed states, not source gaps. Full builds drain and
validate both input streams before atomic publication. A bounded prefix sample
is explicitly not evidence that the unconsumed source tail was accepted.

Semantic churn excludes the first quote of a generation, includes venue-only
and validity-only transitions, and counts a multi-component transition once.
Price and size diagnostics overlap; raw message counts include initialization
and refreshes. Quote freshness is reset by every accepted refresh.

Accepted historical halt intervals close every overlapping source second.
Observable state pauses, including through source changes within a halt, while
cost windows and event origins clear. A partial resume second remains closed;
the next full non-halt source second starts rebuilding. No events from closed
seconds seed the new generation. After resumption, H+1 observations are needed
for fully post-halt provenance, including the movement boundary. Ordinary
source breaks outside halts reset all history. Neither 09:30 nor 16:00 resets
history. Registry use is explicitly historical/ex-post, not live availability.

## Retrieval contract

Publish features for every second. Fit on every eligible September 1 second
from the declared source manifest: this is time-occupancy weighting, not equal
weight per symbol. Never use September 2 to fit, clip, or repair the scaler.
Zero or nonfinite IQR fails. Exact raw order statistics may be sorted on disk
because log1p is monotone, but interpolation occurs AFTER the log transform.

Reference candidates are all eligible September 1 rows. Query candidates are
eligible September 2 endpoints on the 04:00-anchored 300-second grid. Select at
most 128 by smallest SHA256 of `v3-pilot-anchor-1|date|symbol|endpoint_ns`,
breaking hash ties by symbol and timestamp. The cap is configurable before
the run; it is never adjusted based on matches. Return exact top five by
`(distance_squared, reference_symbol, reference_endpoint_ns)`. Same-symbol and
cross-session-segment matches are allowed. No subsequent outcome is used.

Each JSONL match contains query/reference raw coordinates, overall distance,
six family distances, and all 18 weighted squared contributions (summing to
overall squared distance). Mark same-symbol matches within 300 seconds of a
higher-ranked neighbor as overlapping examples. These are not independent
observations. Manifest counts disclose concentration; no IID confidence
intervals, effective independent count, predictive or execution claim is made.

## Resource and publication contract

Stages run single-process under an external live RSS watchdog, counting process
descendants and the supervisor itself, with a 2 GiB working target and termination threshold at 2.8 GiB
(before the 3 GiB hard ceiling). On systems supporting a useful address-space
limit, the worker can additionally set it; macOS virtual address reservations
make RSS monitoring the authoritative check. A killed run is not retried.

Raw replay time is O(N_quotes + N_trades + S); memory is O(B + H), not raw-day
or output-day size. B <= 25,000; H <= 300 plus six movement endpoints. Two
streams each retain at most their current batch, with bounded rollover
temporaries. No raw quote-day state or notional arrays are allocated. Rolling
order statistics use a sorted list of at most H values (O(H) insertion, O(1)
quantile lookup), not per-row window rescans. Compensated sums with an explicit
all-zero reset preserve observed zero after large values expire. Output is
written in buffers of 1,024 rows. Schema metadata binds source/code/contract
and halt identity; the manifest includes source rows, bytes, mtime, SHA-256,
coverage declaration, null counts and eligibility counts.

Scaler population is SQLite on private scratch, with a 32 MiB page cache,
file-backed temporary sorting and memory mapping disabled. Per-coordinate
indexes implement exact quantiles in O(18 R log R) work; disk grows O(18 R),
RAM does not. Feature ingestion buffers at most 4,096 rows. Search is exact
O(18 Q R) work with 16 queries x 4,096 references at once (0.5 MiB distance
matrix plus bounded dimension temporaries); only top-k state survives blocks.
There is no Q x R x 18 tensor or full pairwise matrix. Source/artifact lists
and the small halt registry are metadata, not retained event arrays.
Before search, reserve a conservative `600 * eligible_reference_rows + 2 GiB`
of scratch space for the SQLite population, indexes and temporary sorting.
This is a disk budget, not an allocation or a measured corpus-size estimate.

R2 staging uses the shared verified `staged_symbol_day` helper, one complete
pair at a time with one transfer worker. Require declared canonical coverage
including quote warm-up, plus SHA-256, byte length and row count. Raw scratch
is deleted by its context manager after success/failure; derived artifacts
and manifests persist. No R2 writes or source-data deletion is part of this
pilot. Sources must be frozen in a manifest, including acquisition-universe
selection/provenance; retrospective selection is disclosed.

Each source declares halt-registry coverage dates and the provenance of that
coverage, in addition to the verified registry's content hash. Absence of an
interval in a registry for an unrelated date must not be interpreted as no
halt. Bind explicit source continuity breaks in every output identity.

Before any full symbol-day or corpus run, execute this exact path on a bounded
representative sample. Record consumed rows, elapsed time, peak worker/aggregate
RSS, output/disk sizes, largest allocations and projected full-run costs. Unit
tests alone do not establish memory compliance. Present that measurement and
obtain user confirmation before full external-data acceptance.

## Commands and artifacts

Use `pilot_sources.example.json` as the source-manifest schema; replace every
placeholder with verified provenance. For an existing local canonical pair,
replace `r2: true` with `local_dir: "/absolute/path/to/pair"`. The runner never
deletes that directory. The first read-only real-data proof is:

```sh
python3 src/04_research/run_economic_tape_v3.py \
  --plan /absolute/path/to/pilot_sources.json \
  --output /absolute/path/to/new-proof-output \
  --scratch-root /absolute/path/to/scratch-volume \
  --sample-seconds 900
```

The runner refuses existing outputs and insufficient scratch capacity. It
retains `run_manifest.json`, per-symbol/date feature Parquets, the scaler,
`neighbors/matches.jsonl`, retrieval manifest, and a sibling live resource
measurement. A failed run remains explicitly failed; do not promote partial
feature artifacts. `--features-only` permits a feature-specific diagnostic
without requiring a nondegenerate two-date scaler.

`measure_economic_tape_v3.py` generates bounded synthetic tapes and invokes the
same production feature and search functions under the same watchdog. It
exercises >25,000 quote events in one second and multiple tape regimes. Its
outputs are explicitly synthetic and cannot establish real source acceptance
or market behavior. Its transient raw inputs are removed automatically.
