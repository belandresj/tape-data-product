# Source provenance

The standalone repository was extracted from a dirty research working tree based on source commit `997b28063d9fa01fcff4e978b7acb013ca18255a`. That commit alone cannot reconstruct the copied files. The [initial extraction record](initial-extraction.json) retains their per-file hashes and dependency graph; these describe the initial extraction, not the current working tree.

During extraction, references to unbundled documents in the feature and query contracts were converted to textual provenance references without changing their mathematical definitions. The query implementation identity was expanded to include transitive dependencies. Those changes require new run plans; historical approvals and implementation identities cannot be reused here.

A source-preservation check covered 407 small source/control/document files and Git state. The report changed concurrently in the source workspace; the extraction recorded no source code or Git-state modifications. This was not a complete data backup audit.

The main report and its aggregate supporting assets were added later from the completed research analysis. Their identities are recorded separately in the [publication manifest](../../reports/report_assets/publication_manifest.json). Neither provenance record establishes that the full historical dataset has been rebuilt in this standalone checkout. Git history preserves the original migration notes and extraction inventory.
