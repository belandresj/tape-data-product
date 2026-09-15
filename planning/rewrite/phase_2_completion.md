# Phase 2 implementation checkpoint

Status: synthetic implementation and the authorized bounded KDP/NVDA external-prefix acceptance passed on the project VM on 2026-09-15. Historical vendor retrieval completeness remains explicitly unverified under the owner's accepted policy; this is not a claim of terminal pagination proof or full-corpus readiness.

The installed-source implementation streams projected raw quote/trade batches into `tape_base_1s_v1`, checks composite event ordering and exact scale-9 trade shares, preserves separate quote/trade continuity, and publishes immutable base/context manifests only after schema, grid, hash, and companion checks. Completed exact matches are reusable; corrupt or mismatched outputs fail instead of being overwritten.

Independent literal synthetic checks cover unequal quote exposure (bid/ask/midpoint TWAP 99.75/101.75/100.75), spread and side-specific size integrals, and two exact fractional trades totaling 0.300000000 shares and USD 30.4. Batch sizes 1 and 7 produce identical logical base rows. Path traversal and scaled-state underflow cases fail or remain represented as required.

On the VM, the complete legacy plus new test suite passed 265 tests in 48.35 seconds under a 1,536 MiB cgroup memory ceiling, zero swap, 200% CPU quota, and 900-second runtime cap. Peak cgroup memory was 139.2 MiB. This proves bounded synthetic compatibility, not representative full-day throughput.

The retained KDP/NVDA 2026-09-02 objects were admitted only after rechecking bytes, SHA-256, Parquet row counts, schemas, composite SIP ordering, share units, scale-9 trade quantities, and the recovered historical halt context. The original vendor-pagination receipts remain missing. Version 2 source descriptors therefore retain `terminal_complete=false` and `retrieval_completeness=unverified_missing_original_vendor_pagination_receipts`; the base manifests preserve those fields.

The installed builder emitted 720 rows per member. Independent bounded event integration checked all 48 base fields on all 1,440 rows with zero difference. KDP contained two eligible trades in one second; NVDA contained 1,143 eligible trades across 260 seconds. Build wall time was 0.812 seconds for KDP and 1.462 seconds for NVDA; the larger process-tree RSS peak was 177,831,936 bytes. Base directories total 96,175 bytes including contexts and manifests. This proves the authorized prefix path, not full-session throughput or corpus-scale storage.
