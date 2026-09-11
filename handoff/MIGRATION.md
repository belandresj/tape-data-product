# Transition and preservation decisions

New isolated product development can begin here once standalone verification is complete. Do not redirect an existing job, reuse its approval, point it at this checkout's code, or alter its implementation identity. The original workspace remains available and unchanged by this curation.

Retire the original workspace only after all of these are true:

1. Any overnight job has completed, and its exact source implementation, plan, controls, manifests, result tables and resource evidence have an explicit preservation destination.
2. This repository's tests and synthetic example pass independently, and any real-data workflow required for daily work has separately passed its measured acceptance checkpoint.
3. The approved curated commit has been pushed successfully to the new private remote and the remote commit identity has been verified.
4. Every important excluded category has a preservation decision. In particular, address dirty/untracked source code, the old Git history, private research reports, acquisition receipts, source-release controls, result databases and local-only data before deleting anything.

This task did not audit whether those exclusions are backed up elsewhere. R2 is the intended durable data store, but a bucket's existence does not prove every local artifact is preserved. The historical feature release is roughly 24.9 GB of feature/manifest objects; no corpus copy was attempted. A backup of the full workspace requires a separate size inventory and destination decision. No second archive was created.

## GitHub proposal

- Owner: `belandresj` (authenticated GitHub account checked read-only).
- Name: `tape-data-product` (no accessible existing repository resolved during preparation; recheck immediately before creation).
- Visibility: **PRIVATE**.
- Description: **Direction-neutral equity tape measurements and causal interval retrieval, with bounded processing and reproducible synthetic examples.**
- Fresh history, initial branch `main`; no source history imported and no existing remote modified.
- Approval covers only creating this new private repository and pushing the reviewed local initial commit. Public visibility requires a separate decision.

Before public release, choose source-code licensing, confirm disclosure/redistribution rights for retained aggregate research summaries, and decide whether to finish the broader report. The current report deliberately presents only the completed five-date development pilot; no six-month cohort completion or trading edge is claimed.

Do not treat the local commit as a successful remote backup. The initial publication remains pending until explicit approval and a verified push.
