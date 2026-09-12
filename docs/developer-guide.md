# Development

Use [dataset build and reproduction](dataset-build.md) for installation and the supported stage-by-stage workflow. The package exposes `tape-product`; application code is installed with normal imports.

Run the offline regression suite under live process-tree memory observation:

```sh
python -B scripts/measure.py output/tests.json python -B -m pytest -q
```

Use a fresh evidence filename on each run. Tests use invented fixtures and fake provider/object-store clients; network and sibling-checkout reads are rejected. Small unit fixtures that test calculation or publication behavior supply explicit fake disk capacity; production disk-reserve guards remain active and have separate failure tests.

The [architecture](architecture.md) explains package responsibilities and complexity. The [feature contract](tape_data_product/README.md), [compact layout](compact-layout.md) and [query contract](query-contract.md) define numerical and timing behavior. Changing equations, populations, support, masks, zero/null rules or query activation requires explicit semantic versioning and regression evidence.

Before sharing changes, stage the intended files and run `python scripts/review_contents.py`. It checks the exact Git index for broken documentation links, machine paths, excluded data files, common credential patterns and published figure identities. It is a heuristic check, not a guarantee that every secret can be detected. Implementation plans, coordination records and obsolete implementation prose do not belong in the repository or installed wheel.

The CI workflow runs the offline suite and demo on Python 3.13/Linux. This is configured coverage; the [verification record](verification/README.md) distinguishes environments actually executed locally from CI that has not yet run. No remote publication or GitHub execution is implied by local verification.
