# Bounded member multiprocessing checkpoint

**Status:** implemented and synthetically verified on 2026-09-14. This is a
development checkpoint only. Installed-wheel acceptance, admitted real-data
measurement, corpus readiness, and phase acceptance remain deferred.

## Behavior delivered

Immutable calculation plans accept an exact integer `limits.workers` from 1
through 8. The value remains inside the plan and therefore inside the expected
plan SHA-256; there is no runtime worker override. Duplicate members and bool,
fractional, zero, negative, or greater-than-eight worker counts fail before a
plan directory is created.

The existing calculation runner now uses a fixed set of spawned processes.
Each worker receives descriptor paths, output paths, the feature configuration,
and batch size, then calls the existing base builder followed by the existing
base-to-feature builder for one member. The parent keeps at most one in-flight
member per worker, owns all scheduling and SQLite writes, and schedules
largest admitted stream-row counts first. No Parquet scan is added for cost
estimation. The ledger records each run attempt with the runner-module digest,
spawn strategy, worker count, and scheduling policy.

All Arrow/BLAS computational thread environment variables are set to one before
spawn, and workers verify the inherited limits before importing Arrow builders.
Member and plan locks remain authoritative. A member is marked complete only
after both builders return committed manifests. Ledger state never authorizes
reuse: the builders revalidate completed companions, identities, schemas, and
source inputs. Consequently a completed base can be reused after feature
failure, while a corrupt or mismatched completion still fails closed.

On the first worker error or aggregate guard stop, the parent dispatches no
further members, marks the failing/active ledger records accurately, terminates
the active worker set, joins with bounded waits, and kills any survivor. Linux
workers arm `PR_SET_PDEATHSIG` before importing numerical code, so abrupt
parent termination also terminates computation; a later run changes stale
`running` ledger rows to `interrupted` before performing normal output reuse
checks. Valid committed outputs and unrelated attempt directories are retained.

Time, summed process-tree RSS, owned output/attempt growth, and free disk are
sampled and enforced as aggregate run limits, not multiplied by worker count.
The existing production free-disk reserve is checked throughout execution.
The RSS measure intentionally sums worker RSS and therefore conservatively
double-counts shared pages.

## Synthetic correctness

Focused runner and endpoint-pipeline tests cover:

- decoded one-worker versus four-worker equality for base, feature, and support
  rows, including keys, nulls, masks, counts, Decimal values, and floats;
- mixed-size largest-first scheduling with every member represented once;
- verified completed-output reuse, base-only restart, and stale-running ledger
  reconciliation;
- one worker failing while another is active, with pending work undispatched;
- abrupt worker exit and parent termination, with no surviving computation and
  successful restart;
- aggregate RSS stop across two active workers;
- invalid worker counts, duplicate plan members, and concurrent plan locking.

The focused endpoint pipeline completed 44 tests in 22.79 seconds before final
instrumentation changes; affected focused tests were rerun after those changes.
The final full regression suite passed all 320 tests in 80.40 seconds. Its
enclosing cgroup peaked at 446.4 MiB with no swap.

## Synthetic scaling measurement

The deterministic workload contains 16 independent members:

- eight event-heavy members: 3,600 output seconds and 50,000 quote events each;
- eight longer-history members: 18,000 output seconds and 600 quote events each;
- two trades per member.

Each worker-count run therefore processes 404,832 raw events and emits 172,800
member-seconds through the integrated plan preflight, spawn, base replay,
feature calculation, committed manifests, and SQLite ledger path. Fixture
generation was outside timing. Every measurement used fresh output roots.

The four runs shared a systemd service ceiling of 8 GiB RAM, no swap, 128 tasks,
an eight-vCPU quota, and a ten-minute deadline. Plans also enforced an 8 GiB
summed process-tree RSS stop, 4 GiB aggregate owned-output/scratch cap, 600
seconds, and the 80 GiB production free-disk reserve.

| Workers | Wall (s) | Speedup | Efficiency | Raw events/s | Output member-s/s | Tree RSS peak | Aggregate CPU |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 182.61 | 1.00x | 100.0% | 2,217 | 946 | 305.8 MiB | 120% |
| 2 | 90.32 | 2.02x | 101.1% | 4,482 | 1,913 | 485.0 MiB | 216% |
| 4 | 47.09 | 3.88x | 97.0% | 8,597 | 3,670 | 835.0 MiB | 407% |
| 8 | 25.72 | 7.10x | 88.7% | 15,738 | 6,717 | 1,532.9 MiB | 717% |

Eight workers had the best measured throughput. Peak owned output plus active
attempt growth was 29.6 MB; the complete private benchmark tree, including four
separate source copies and all outputs, was 162 MB. The enclosing cgroup
reported a 758.5 MiB memory peak; summed process-tree RSS is higher because
shared pages are counted once per process.

Host CPU busy time rose from 15.1% at one worker to 90.4% at eight on the
eight-vCPU VM. Host I/O wait remained between 0.005% and 0.029%. Process-tree
physical reads were zero because the just-generated inputs were served from
page cache; process-tree writes were about 30.9 MB per run. These measurements
show no storage bottleneck in this warm synthetic workload, but they do not
test cold real Parquet reads.

The recommended candidate for the next bounded real-data comparison is eight
workers, retaining the same aggregate guards and comparing against four if
real member RSS or cold-read contention differs materially.

## Remaining limitations

This workload is synthetic, warm-cache, quote-heavy, and has only two trades
per member. It does not represent real event distributions, cold storage,
source defects, full-session compression, or real validation/hash overhead.
The 7.10x result is not a corpus runtime estimate and must not be combined with
earlier independent synthetic speedups.

The Linux kernel parent-death guarantee is used on the project VM. Catchable
errors and interrupts use portable explicit termination, but another operating
system cannot provide the same hard-parent-death behavior through this Linux
mechanism. The runner remains a single-host, one-plan-lock execution system
with no automatic retries or distributed scheduling. Fail-fast preserves work
already committed by another active member; it does not roll back valid output.

No real corpus files were read, no installed release was changed, and no
external data, R2, download, publication, admission audit, or corpus execution
was performed. The next decision remains whether installed-release and bounded
real-data measurements at four/eight workers support the four-hour target or
identify a concrete need for native replay/feature kernels.
