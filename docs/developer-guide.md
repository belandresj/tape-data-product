# Development

Use Python 3.13. Build and install a wheel rather than importing from another repository or an installed release directory. Runtime dependencies are declared in `pyproject.toml`; the verified Linux closure is hash-pinned in `config/vps-python313-linux.lock`.

The primary command is:

```text
tape-product --help
```

For focused changes, run the tests owning the affected package and at least one installed-package CLI smoke check. Calculation-runner resource tests are Linux-specific because they use process-tree I/O counters and worker controls. Report-only changes should verify artifact hashes, local Markdown links, numerical/table reconciliation, and the rendered figures they affect.

Semantic changes to clocks, event populations, units, masks, coverage, resets, or feature equations require a new versioned contract and regression evidence. Do not silently reinterpret an existing release identity. Integrity verification, independent numerical reconstruction, descriptive market evidence, and executable trading expectancy remain separate claims.

Generated datasets, credentials, private catalogs, operational receipts, and temporary analysis belong outside Git. The public repository retains code, current contracts and usage documentation, focused tests, sanitized aggregate report data, and final figures.
