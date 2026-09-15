# Real-data overnight readiness measurement — 2026-09-15 Pacific

**Status:** the integrated installed release passed a bounded seven-member real-data run at explicit batch size 4,096. A 5,208-member historical population is metadata-admitted and has an immutable proposed plan, but the plan is intentionally blocked on the owner's population/resource/readiness decision. No corpus job was launched or scheduled.

## Result and operational conclusion

The production scheduler completed seven full-session members containing 13,120,453 raw events and 403,200 member-seconds in 81.06 seconds. The small sample was deliberately heterogeneous and imbalanced: six members completed in 10.6–17.4 seconds while the 10.8-million-event SPCX member completed in 81.3 seconds. That imbalance makes the observed 4,974 member-seconds/s an intentionally conservative sample throughput, not a direct corpus rate.

A two-term model separates raw replay from dense one-second work. The fitted serial base model is 2.535 seconds/member plus 7.155 microseconds/raw event; the measured feature mean is 7.961 seconds/member. For all 7,085 members this projects 8.90 one-worker hours of base replay and 15.67 one-worker hours of feature work. At eight workers with 80–95% scheduling efficiency, calculation is 3.23–3.84 hours (central 3.41 hours at 90%). The measured strict installed verifier adds about 4.39 hours if run serially. Including normal startup/finalization and cache uncertainty, full 7,085-member calculation plus verification has a central estimate near 7.9 hours and a conservative planning range of roughly 7.7–9.0 hours. Completion within eight hours is plausible but uncertain; completion within ten hours is supported.

The proposed 5,208-member admitted population contains 1,215,040,615 raw events and 299,980,800 member-seconds. Its modeled calculation is 2.32–2.75 hours (central 2.44), and measured-rate serial verification adds 3.23 hours. Allowing overhead and cache uncertainty gives roughly 5.7–7.0 hours, supporting completion within eight hours. If an eight-hour service stop interrupts calculation, every committed member remains reusable; the model expects all calculation outputs to be committed well before the stop, with at most a verification tail remaining.

Feature calculation remains the largest build component across the population (15.67 one-worker hours versus 8.90 for replay). The maximum-event SPCX path is the opposite: its measured base/feature times were 79.89/7.17 seconds, so replay dominates isolated event-heavy members. If verification remains a separate serial pass, it is the largest wall-clock acceptance stage after eight-worker calculation.

## Sample and measurement

The fixed serial sample ran first through installed public builders. The concurrent sample added ordinary-low, ordinary-mid, ordinary-high, trade-heavy, and busy candidates selected from inventory/admission/footer evidence. Fixed WETO 2026-08-18 failed admission with `invalid halt interval/identity` and was not replaced, leaving seven admitted members and seven actual workers.

| Member | Role | Raw events | Serial base | Serial features | Concurrent base-completion estimate | Concurrent feature delta | Output bytes |
|---|---|---:|---:|---:|---:|---:|---:|
| GOSS 2026-07-31 | ordinary low | 50,962 | — | — | 3.34 s | 7.23 s | 13,166,499 |
| BTTC 2026-08-20 | ordinary mid | 119,519 | — | — | 3.89 s | 7.15 s | 14,395,068 |
| KPTI 2026-07-31 | median | 133,511 | 3.37 s | 8.26 s | 3.94 s | 7.12 s | 13,274,493 |
| OMH 2026-07-27 | ordinary high | 308,321 | — | — | 5.29 s | 7.45 s | 16,120,852 |
| GLE 2026-05-07 | p90 | 681,857 | 7.54 s | 8.46 s | 7.86 s | 7.23 s | 17,849,863 |
| VSME 2026-06-12 | busy/p95 | 1,014,083 | — | — | 10.11 s | 7.26 s | 16,242,185 |
| SPCX 2026-06-12 | maximum | 10,812,200 | 79.89 s | 7.17 s | 75.28 s | 6.05 s | 11,110,425 |

Concurrent stage estimates use committed-manifest timestamps relative to scheduler start; total runner wall is authoritative. The runner used largest-first scheduling, spawned seven workers, and set all Arrow/BLAS computational thread limits to one. It recorded 170.93 process CPU-seconds, equivalent to 2.11 cores on average or 26.4% of the eight-vCPU host over the imbalanced run. Summed process-tree RSS peaked at 1,762,496,512 bytes. Process counters recorded 122,372,096 physical read bytes and 102,735,872 write bytes. High-resolution host I/O-wait was not captured; the available ten-minute system sample does not isolate the 81-second run.

Cache state was mixed and not controlled. The three fixed members were warm because the serial run immediately preceded concurrency; the five added candidates' cache state was unknown. Physical reads were much lower than 981 MB logical reads, so this should not be represented as cold-cache evidence. No cache was flushed and no cold-cache experiment was added.

Strict installed base and feature verification passed all seven completed concurrent members in 15.63 seconds. For KPTI, GLE, and SPCX, 30 fixed startup/active/late rows per table matched retained accepted independently reconstructed outputs: base fields matched exactly and floating differences remained within `rtol=2e-10, atol=1e-9`. This is integrated verification plus sampled comparison, not independent reconstruction of every pilot row.

## Population and capacity

The canonical transport population remains 7,085 paired members, approximately 1.968 billion raw events, and 408,096,000 conditional full-session member-seconds. Batched descriptor admission reconciled the 6,222-member historical population exactly:

- 5,208 metadata-admitted now;
- 1,014 blocked by invalid halt interval/identity records;
- 863 outside the retained historical selection/halt context;
- consumption-time composite order, scale-9 trade precision, and source-stability checks remain pending for every unbuilt admitted member and are enforced by the builder.

Across the seven-member sample, output averaged 14,594,198 bytes/member: 2,748,437 base, 9,448,751 features, 2,386,776 support, and 10,233 metadata bytes. For 5,208 members the central estimate is 76.01 GB (14.31 GB base, 49.21 GB features, 12.43 GB support, 0.05 GB metadata); the observed-member low/high envelope is 57.86–92.96 GB. For all 7,085 members the corresponding central estimate is 103.40 GB and the envelope is 78.72–126.47 GB.

Current free space after the pilot was approximately 145.48 GB. The proposed admitted-population limits are a 20 GiB free-disk floor, a 96 GiB aggregate cap counting committed output growth plus active attempts, eight workers, an 8 GiB cgroup memory ceiling with no swap, a 7 GiB summed-RSS stop, one computational thread per worker, batch size 4,096, and an eight-hour runner deadline. The 96 GiB cap covers the admitted-population high envelope plus concurrent attempts while preserving materially more than the 20 GiB floor. The complete 7,085-member high envelope would not fit under the current 20 GiB floor and is not the proposed population.

## Release and proposed plan

The isolated installed release is source revision `71059c2c14d9f2959362d9975f08ba479daa5513`; wheel SHA-256 `200867c43bae8bad7d1762ce47d34a5e4b9ce31a9c9eafd5ebf94117a43ae32b`. Imports resolved outside the checkout to `/opt/tape-data-product/releases/overnight-71059c2/venv`, and the required base, feature, calculate-plan, and calculate-run CLI interfaces loaded successfully. `/opt/tape-data-product/current` was unchanged.

The immutable proposed 5,208-member plan is `/srv/tape-data-product/control/overnight-readiness-71059c2-20260915/population-proposed-plan/plan.json`, SHA-256 `2383f599f7b928442bc577592365bc9a34d7aeeed7a2049450645c4421730c43`. Its readiness decision is deliberately `proposed_pending_owner`; a read-only installed preflight reports only `reviewed_readiness_decision_identity_mismatch`. After owner approval, create a new immutable accepted decision and plan rather than editing this proposal, then launch manually under the stated systemd cgroup with the newly reported plan hash. No current launch command is valid until that acceptance artifact exists.
