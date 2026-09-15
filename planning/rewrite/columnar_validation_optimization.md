# Columnar Arrow validation optimization checkpoint

**Baseline:** VM revision `b26c0e6` (a verified descendant of cleanup revision
`af7b7f4`). This is a development checkpoint, not installed-release or
real-data acceptance.

## Change

`contracts.validation.validate_batch` now validates base, feature, and support
record batches with bounded Arrow compute kernels and a zero-copy NumPy view of
the required timestamp column. It no longer converts every column to a Python
list or reconstructs a dictionary per row on valid input. Schema/configuration
plans are cached for the default and supported alternate configurations.
Integer and Decimal columns remain in Arrow throughout validation.

All boolean reductions fill unresolved null predicates with false, so a null
cannot hide an invalid row. Nullable guarded relationships use Kleene boolean
operations so an explicitly false guard still dominates an unavailable
component. The validator remains limited to 25,000 rows and returns the same
final member/grid key.

The prior scalar implementation is retained only under `tests/` as a
differential reference; there is one production validator backend. Because
`validation.py` was already part of base and feature implementation identity
inputs, this change updates both implementation identities without changing
the semantic contract or schemas.

## Correctness evidence

Focused contract, pipeline, and differential verification: **81 passed in
10.77 seconds**. The 21 new cases compare scalar and columnar acceptance and
returned keys while also asserting literal outcomes for representative
corruptions. They cover:

- base, feature, and support tables; empty, single-row, 4,096-row, 25,000-row,
  and over-limit batches;
- null/mask mismatches, disallowed reason bits, NaN/infinity, exact Decimal
  quantities, and both sides of the established TWAP and ratio tolerances;
- exposure, source, halt, endpoint-size, midpoint-age, feature dependency,
  support/count, and nullable-reduction relationships;
- within- and cross-batch gaps, duplicates, member changes, and the returned
  final key;
- alternate EW/age configurations.

The existing endpoint contract/pipeline suite passed as part of that focused
run. The complete repository regression suite then passed: **303 passed in
52.29 seconds**. Its first attempt had one development-environment package
metadata failure because the new venv had not installed the checkout; after an
editable no-dependency install, the specific identity check and full suite
passed.

## Microbenchmark

One worker/process and one computational thread; Python 3.13.15, PyArrow
22.0.0, NumPy 2.3.4. Each backend received the same prebuilt deterministic
4,096-row Arrow batches, with valid and unavailable/null patterns. Fixture
generation was outside the timed region. Both implementations were warmed
three times; figures below are medians of nine complete `validate_batch`
calls.

| Table | Scalar median | Columnar median | Scalar rows/s | Columnar rows/s | Speedup |
|---|---:|---:|---:|---:|---:|
| base | 191.829 ms | 6.121 ms | 21,352 | 669,123 | 31.34x |
| features | 195.443 ms | 5.317 ms | 20,958 | 770,430 | 36.76x |
| support | 113.605 ms | 2.287 ms | 36,055 | 1,790,646 | 49.66x |

Separate 25-repeat processes reported maximum RSS of 221,356 KiB for scalar
and 221,296 KiB for columnar. Fixture construction and imports are included in
those process peaks, so this is evidence of no notable overall peak-RSS
increase, not a precise attribution of temporary kernel memory.

## Limitation and deferred acceptance

This benchmark isolates validator cost on synthetic in-memory batches. It does
not measure end-to-end replay, feature calculation, Parquet I/O, a real
symbol-day, multi-worker scaling, or full-corpus runtime. No wheel was built
and no installed release or real-data acceptance was performed. Those remain
deferred to the combined optimization checkpoint.
