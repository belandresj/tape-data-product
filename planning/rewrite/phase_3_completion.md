# Phase 3 implementation checkpoint

Status: base-only default-feature implementation and bounded synthetic checks passed on the project VM on 2026-09-14; external reconstruction and corpus-launch readiness remain blocked.

The feature builder consumes only verified base/context companions. It maintains exponent-scaled nonnegative EW sums, a six-endpoint lag ring, independently weighted spread/activity/side-size numerators and exposures, and bounded exact-age windows. It writes both default half-life views, all six age-p90 fields, and the full support schema. Feature manifests bind the immutable base-manifest hash, feature configuration, schemas, and scoped feature implementation identity.

Synthetic checks verify 60/300-second startup boundaries, an h=1 explicit squared-moment hand sum, p90 interpolation `[0,1,2,3] -> 2.7`, positive state after more than 1,100 binary halvings, and raw batch-size independence. The full pre-existing test suite remains green, so legacy compact identities and CLI paths were not replaced.

The two authorized 720-second real prefixes were not run because source admission is unresolved (see Phase 2 completion). Consequently there is no valid real-prefix independent reconstruction, busy-member throughput measurement, or compressed full-session output measurement. The full-run projection is insufficient. The smallest useful extension after admission evidence is resolved is still the already authorized KDP/NVDA prefix; a later representative busy full-session measurement would require separate approval before using it for a 7,085-member corpus projection.

The proposed corpus contains 7,085 members and 408,096,000 dense rows. Every additional compressed 100 bytes per row consumes approximately 40.8 GB. Without representative full-session compression and busy-event throughput, no defensible runtime or disk upper bound exists. `calculate run` therefore remains fail-closed and must not be scheduled or launched.
