# Phase 3 implementation checkpoint

Status: the base-only default-feature implementation, bounded synthetic checks, and authorized KDP/NVDA external-prefix reconstruction passed on the project VM on 2026-09-15. Corpus-launch readiness remains pending.

The feature builder consumes only verified base/context companions. It maintains exponent-scaled nonnegative EW sums, a six-endpoint lag ring, independently weighted spread/activity/side-size numerators and exposures, and bounded exact-age windows. It writes both default half-life views, all six age-p90 fields, and the full support schema. Feature manifests bind the immutable base-manifest hash, feature configuration, schemas, and scoped feature implementation identity.

Synthetic checks verify 60/300-second startup boundaries, an h=1 explicit squared-moment hand sum, p90 interpolation `[0,1,2,3] -> 2.7`, positive state after more than 1,100 binary halvings, and raw batch-size independence. The full pre-existing test suite remains green, so legacy compact identities and CLI paths were not replaced.

The installed builder calculated both default feature views and support tables for the two authorized 720-second real prefixes. Independent explicit-history reconstruction checked all 51 feature fields and all 37 support fields on all 1,440 rows per table. Maximum absolute floating-point differences were `2.6712e-13`/`1.4211e-13` for KDP features/support and `1.8190e-11`/`1.1369e-13` for NVDA. All comparisons passed the `2e-10` relative and `1e-9` absolute tolerances.

Feature build wall time was 1.081 seconds for KDP and 1.080 seconds for NVDA; the larger process-tree RSS peak was 172,466,176 bytes. Feature directories total 521,836 bytes including manifests, or 362.39 bytes per dense output second across feature and support artifacts. Both symbols have 661 fast-view and 421 slow-view valid RMS/spread rows, exactly matching the 60/300-second startup rules.

Installed verification uses Python 3.13.15 outside the checkout and an isolated wheel built from the reviewed implementation commit. The production CLI integrity checks passed for all six real Parquet companions. The external reference verifier separately reconstructed raw intervals and explicit histories rather than calling the production accumulators.

The proposed corpus contains 7,085 members and 408,096,000 dense rows. Every additional compressed 100 bytes per row consumes approximately 40.8 GB. The 12-minute session-start sample is still insufficient to bound full-session compression, long-history behavior, or busy-member throughput. Production multi-worker ownership/restart behavior remains intentionally unimplemented. No new full-corpus plan has been accepted, and `calculate run` must not be scheduled or launched.
