# Development

Use [dataset build and reproduction](dataset-build.md) for installation and the supported stage-by-stage workflow. The package exposes `tape-product`; application code is installed with normal imports.

Run the offline regression suite under live process-tree memory observation:

```sh
python -B scripts/measure.py output/tests.json python -B -m pytest -q
```

Use a fresh evidence filename on each run. Tests use invented fixtures and fake provider/object-store clients; network and sibling-checkout reads are rejected. Small unit fixtures that test calculation or publication behavior supply explicit fake disk capacity; production disk-reserve guards remain active and have separate failure tests.

The [architecture](architecture.md) explains package responsibilities and complexity. The [V1 feature contract](reference/v1/feature-contract.md), [compact layout](reference/v1/compact-layout.md) and [query contract](reference/v1/query-contract.md) define the implemented numerical and timing behavior. Changing equations, populations, support, masks, zero/null rules or query activation requires explicit semantic versioning and regression evidence.

Before sharing changes, stage the intended files and run `python scripts/review_contents.py`. It checks the exact Git index for broken documentation links, machine paths, excluded data files, common credential patterns and published figure identities. It is a heuristic check, not a guarantee that every secret can be detected. Implementation plans, coordination records and obsolete implementation prose do not belong in the repository or installed wheel.

The CI workflow installs a wheel, runs the offline suite, and runs the guarded demo outside the checkout on Python 3.13/Linux. This is configured coverage; the [legacy V1 verification record](../reports/verification/legacy-v1/README.md) distinguishes the environment actually executed from CI that had not yet run. No remote publication or GitHub execution is implied by local verification.
