"""Explicit zero, underflow, finite-bin and overflow assignments."""

import numpy as np


def bin_ids(values, axis):
    edges = np.asarray(axis["edges"])
    good = np.isfinite(values)
    if (
        np.any(values[good] < 0)
        or (axis["feature"] == "movement_participation" and np.any(values[good] > 1))
        or np.isinf(values).any()
    ):
        raise ValueError("Invalid nonnegative feature")
    ids = np.searchsorted(edges, values, side="right") + 1
    ids[values == edges[-1]] = len(edges)
    ids[values == 0] = 0
    ids[~good] = -1
    return ids
