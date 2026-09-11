#!/usr/bin/env python3
"""Neutral observable-time clock for Rolling Tape State V2.

Accepted halt rows do not advance this clock.  The helper consumes an already
accepted registry overlay; it never detects or infers halts from market data.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np


NS = 1_000_000_000


@dataclass(frozen=True)
class HaltClock:
    observed_row_indices: np.ndarray
    wall_row_to_latest_observed_position: np.ndarray
    state_epoch_id: np.ndarray
    state_epoch_start_position: np.ndarray
    halt_generation: np.ndarray
    resume_boundary_on_observed_clock: np.ndarray


def build_halt_clock(
    interval_end_ns: np.ndarray,
    continuity_segment_id: np.ndarray,
    halt_interval_active: np.ndarray,
    halt_resume_boundary: np.ndarray,
) -> HaltClock:
    """Construct V2's fast/slow observation clock for one symbol-day.

    Ordinary wall-grid gaps and continuity changes outside halts reset the
    state epoch.  A continuity change accumulated while an accepted halt is
    active is treated as part of that accepted halt and does not discard the
    retained fast/slow history.
    """

    ends = np.asarray(interval_end_ns, dtype=np.int64)
    continuity = np.asarray(continuity_segment_id, dtype=np.int64)
    active = np.asarray(halt_interval_active, dtype=np.bool_)
    resume = np.asarray(halt_resume_boundary, dtype=np.bool_)
    n = ends.size
    if not (continuity.size == active.size == resume.size == n):
        raise ValueError("halt-clock inputs must have equal length")
    if n and np.any(ends[1:] <= ends[:-1]):
        raise ValueError("halt-clock interval_end must be strictly increasing")

    observed = np.flatnonzero(~active).astype(np.int64)
    wall_to_latest = np.full(n, -1, dtype=np.int64)
    if n:
        counter = np.cumsum(~active, dtype=np.int64) - 1
        wall_to_latest[:] = counter

    m = observed.size
    epoch = np.zeros(m, dtype=np.int64)
    epoch_start = np.zeros(m, dtype=np.int64)
    generation = np.zeros(m, dtype=np.int64)
    observed_resume = resume[observed].copy()
    current_epoch = 0
    current_start = 0
    current_generation = 0

    for pos, row in enumerate(observed):
        if pos == 0:
            observed_resume[pos] = False
        else:
            prev_row = int(observed[pos - 1])
            wall_grid_gap = np.any(np.diff(ends[prev_row : row + 1]) != NS)
            crossed_halt = bool(np.any(active[prev_row + 1 : row]))
            is_resume = bool(resume[row] or crossed_halt)
            observed_resume[pos] = is_resume
            if is_resume:
                current_generation += 1
            ordinary_continuity_change = (
                not crossed_halt and continuity[row] != continuity[prev_row]
            )
            if wall_grid_gap or ordinary_continuity_change:
                current_epoch += 1
                current_start = pos
        epoch[pos] = current_epoch
        epoch_start[pos] = current_start
        generation[pos] = current_generation

    return HaltClock(
        observed_row_indices=observed,
        wall_row_to_latest_observed_position=wall_to_latest,
        state_epoch_id=epoch,
        state_epoch_start_position=epoch_start,
        halt_generation=generation,
        resume_boundary_on_observed_clock=observed_resume,
    )

