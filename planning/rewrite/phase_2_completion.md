# Phase 2 implementation checkpoint

Status: synthetic implementation checkpoint passed on the project VM on 2026-09-14; retained external-prefix acceptance is blocked by source evidence.

The installed-source implementation streams projected raw quote/trade batches into `tape_base_1s_v1`, checks composite event ordering and exact scale-9 trade shares, preserves separate quote/trade continuity, and publishes immutable base/context manifests only after schema, grid, hash, and companion checks. Completed exact matches are reusable; corrupt or mismatched outputs fail instead of being overwritten.

Independent literal synthetic checks cover unequal quote exposure (bid/ask/midpoint TWAP 99.75/101.75/100.75), spread and side-specific size integrals, and two exact fractional trades totaling 0.300000000 shares and USD 30.4. Batch sizes 1 and 7 produce identical logical base rows. Path traversal and scaled-state underflow cases fail or remain represented as required.

On the VM, the complete legacy plus new test suite passed 254 tests in 41.95 seconds under a 1,536 MiB cgroup memory ceiling, zero swap, 200% CPU quota, and 900-second runtime cap. Peak cgroup memory was 142.4 MiB. This proves bounded synthetic compatibility, not representative full-day throughput.

The retained KDP/NVDA 2026-09-02 objects have verified bytes, hashes, row counts, and SIP order. Their retained records explicitly say transport verification did not create historical coverage receipts. The migration inventory also records missing quote-unit and trade-precision evidence for every member and unresolved terminal coverage for 994 members. No identity-bound accepted halt/continuity record was located for the retained prefix. Therefore the real prefix was not decoded or built: doing so would require inventing admission evidence forbidden by the accepted contract.
