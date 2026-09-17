# Historical data access

The reported market observations came from Massive.com and were retained in a private Cloudflare R2 store. R2 is storage, not the market-data source. This repository does not contain vendor credentials, private catalogs, detailed trade/quote rows, or access to the author’s bucket.

The V2 report covers 5,208 completed symbol-days, 122 trading dates, and 299,980,800 represented one-second rows. The tracked population aggregate is bound to inventory SHA-256 `766564b167a7a8b0987b7b3ceae5a1efdad5fe4ee029912ea28b43d92c6bb3d7`. The full-release analysis used reference identity `a1fab9c41bb51122ad49f9976f79b125a5542d2c2d4c73c4c2b0a23684d787ea`, contract identity `bf4d8c1211bf096d9b0ab3b2c0d62f3858ff1da738b42aa8c3ed4cba67020bd1`, and source revision `ea2e16128225a91ceb6003fcb3f6ef80985ba8e0`.

Exact reproduction requires the original canonical T/Q bytes, their source/admission evidence, member contexts, halt and continuity overlays, accepted calculation configuration, and completed manifests. Reacquiring the same symbols and dates creates a new source snapshot; it does not prove byte-for-byte equality with the report release. The original vendor pagination completion is not independently verified for the complete historical lineage, as disclosed in the report.

New users can acquire data with their own Massive.com credentials and operate entirely on local files. R2 is optional and is used only by explicit storage commands configured for the user’s own endpoint, bucket, and credentials. Existing remote objects are immutable: their hashes, byte lengths, row counts, and metadata must match, and publication never overwrites them.

Canonical inputs are organized as `tq/session_date=YYYY-MM-DD/symbol=SYMBOL/{trades,quotes}.parquet` with an identity-bound source-pair descriptor and member context. See [acquisition and canonical storage](acquisition.md) for source normalization and [dataset build and reproduction](dataset-build.md) for the installed pipeline.

Installation does not authorize acquisition, upload, deletion, or a full-corpus rebuild. Those operations require explicit credentials, storage scope, resource limits, and input identities.
