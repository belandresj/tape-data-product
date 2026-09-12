"""Bounded numerical aggregates for automatic report-plot regeneration.

One projected pass is O(NF + NP), with O(BF + PJ^2) RAM. Exact tables are
then built one feature at a time with external O(N log N) sorting. Retained
extract/table disk is O(NF). Contributor accounting is disk-backed SQLite.
At most 10,000 members are admitted. Extract writers close at each member;
each has at most 1,024 row groups, bounding Arrow footer metadata. The path
inventory is therefore capped at 10,000 strings, not proportional to endpoints.

This module consumes already calculated rolling fields. It never replays raw
T/Q, recomputes rolling histories, renders plots, or mutates source products.
"""

from collections import OrderedDict
import json
from pathlib import Path
import sqlite3

import numpy as np

from . import selection as Selection

FEATURE_STEMS = (
    "movement_mean_5s_bps",
    "movement_participation",
    "quoted_spread_mean_bps",
    "trade_rate",
    "dollar_rate",
    "trade_age_p90_seconds",
    "quote_age_p90_seconds",
    "midpoint_change_age_p90_seconds",
    "movement_mean_to_spread",
)
FEATURES = tuple(f"{stem}_{h}s" for h in Selection.HORIZONS for stem in FEATURE_STEMS)
MIDPOINT_STATUSES = ("known", "no_change_observed", "unobservable")
ZERO_TOTAL_MOVEMENT_REASON = 128
MAX_MEMBERS = 10000
MAX_EXTRACT_ROW_GROUPS = 1024

JOINT_STEMS = OrderedDict(
    (
        ("movement_spread", ("quoted_spread_mean_bps", "movement_mean_5s_bps")),
        ("movement_participation", ("movement_mean_5s_bps", "movement_participation")),
        ("movement_trade_rate", ("trade_rate", "movement_mean_5s_bps")),
    )
)
BANDED_JOINT_STEMS = OrderedDict(
    (
        (
            "movement_spread_rate_bands",
            ("quoted_spread_mean_bps", "movement_mean_5s_bps"),
        ),
        (
            "movement_participation_rate_bands",
            ("movement_mean_5s_bps", "movement_participation"),
        ),
    )
)


def _write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("x") as handle:
        json.dump(value, handle, indent=2, sort_keys=True, allow_nan=False)
        handle.write("\n")
    temporary.replace(path)


def _session_and_pooled(array, names):
    result = {}
    for i, session in enumerate(Selection.SESSIONS):
        result[session] = {name: int(array[i, j]) for j, name in enumerate(names)}
    result["pooled"] = {
        name: sum(result[s][name] for s in Selection.SESSIONS) for name in names
    }
    return result


def _validate_axis(axis, stem):
    edges = np.asarray(axis["edges"], dtype=np.float64)
    if (
        edges.ndim != 1
        or not 2 <= len(edges) <= 513
        or not np.all(np.isfinite(edges))
        or not np.all(np.diff(edges) > 0)
    ):
        raise ValueError("invalid report axis: " + stem)
    if stem == "movement_participation" and (edges[0] != 0 or edges[-1] != 1):
        raise ValueError("participation axis must span [0,1] exactly")
    return edges


def axis_registry(axis_configuration):
    """Accept the frozen pair-atlas axis config or a test-sized explicit map."""
    axes = axis_configuration.get("axes", axis_configuration)
    required = {
        f
        for pair in (*JOINT_STEMS.values(), *BANDED_JOINT_STEMS.values())
        for f in pair
    }
    if not required <= set(axes):
        raise ValueError("axis configuration lacks report joint features")
    result = {}
    for stem, axis in axes.items():
        edges = _validate_axis(axis, stem)
        item = dict(axis, feature=stem, edges=edges.tolist())
        item.setdefault("bins", len(edges) + 2)
        if item["bins"] != len(edges) + 2:
            raise ValueError(
                "report axes require zero, underflow, finite, and overflow bins"
            )
        result[stem] = item
    return result


class FixedJoint:
    """Exact fixed-bin joint counts with explicit tails and zero atoms."""

    METRICS = (
        "eligible",
        "off_axis",
        "x_below",
        "x_above",
        "y_below",
        "y_above",
        "x_zero",
        "y_zero",
        "zero_zero",
    )

    def __init__(self, x_axis, y_axis, banded=False):
        self.x_axis, self.y_axis = x_axis, y_axis
        self.x_edges = np.asarray(x_axis["edges"], dtype=np.float64)
        self.y_edges = np.asarray(y_axis["edges"], dtype=np.float64)
        lead = (3, 4) if banded else (3,)
        self.banded = banded
        self.counts = np.zeros((*lead, x_axis["bins"], y_axis["bins"]), dtype=np.int64)
        self.metrics = {name: np.zeros(lead, dtype=np.int64) for name in self.METRICS}

    def add(self, x, y, sessions, population, bands=None):
        if bands is None and self.banded or bands is not None and not self.banded:
            raise ValueError("joint band configuration mismatch")
        if np.isinf(x).any() or np.isinf(y).any():
            raise ValueError("joint values contain infinity")
        if np.any(x[population] < 0) or np.any(y[population] < 0):
            raise ValueError("joint values must be non-negative")
        from . import axes as PairAxes

        x_bins = PairAxes.bin_ids(x, self.x_axis)
        y_bins = PairAxes.bin_ids(y, self.y_axis)
        for session in range(3):
            band_range = range(4) if self.banded else (None,)
            for band in band_range:
                mask = population & (sessions == session)
                if band is not None:
                    mask &= bands == band
                xx, yy = x[mask], y[mask]
                key = (session, band) if band is not None else (session,)
                n = len(xx)
                self.metrics["eligible"][key] += n
                self.metrics["x_zero"][key] += int(np.count_nonzero(xx == 0))
                self.metrics["y_zero"][key] += int(np.count_nonzero(yy == 0))
                self.metrics["zero_zero"][key] += int(
                    np.count_nonzero((xx == 0) & (yy == 0))
                )
                xb, yb = x_bins[mask], y_bins[mask]
                tails = {
                    "x_below": xb == 1,
                    "x_above": xb == self.x_axis["bins"] - 1,
                    "y_below": yb == 1,
                    "y_above": yb == self.y_axis["bins"] - 1,
                }
                for name, selected in tails.items():
                    self.metrics[name][key] += int(np.count_nonzero(selected))
                off = (
                    tails["x_below"]
                    | tails["x_above"]
                    | tails["y_below"]
                    | tails["y_above"]
                )
                self.metrics["off_axis"][key] += int(np.count_nonzero(off))
                target = self.counts[key]
                flat = xb * self.y_axis["bins"] + yb
                target.reshape(-1)[:] += np.bincount(
                    flat, minlength=target.size
                ).astype(np.int64)
        self.validate()

    def validate(self):
        total = self.counts.sum(axis=(-2, -1), dtype=np.int64)
        if not np.array_equal(total, self.metrics["eligible"]):
            raise ValueError("joint histogram mass does not equal its denominator")
        if np.any(self.metrics["zero_zero"] > self.metrics["x_zero"]) or np.any(
            self.metrics["zero_zero"] > self.metrics["y_zero"]
        ):
            raise ValueError("joint zero accounting is inconsistent")

    def summary(self):
        if self.banded:
            result = {}
            for band, label in enumerate(Selection.RATE_BAND_LABELS):
                result[label] = _session_and_pooled(
                    np.stack(
                        [self.metrics[name][:, band] for name in self.METRICS], axis=1
                    ),
                    self.METRICS,
                )
            return result
        return _session_and_pooled(
            np.stack([self.metrics[name] for name in self.METRICS], axis=1),
            self.METRICS,
        )


class ReportPlotAggregates:
    """Streaming aggregation state plus a narrow gate-eligible exact extract."""

    def __init__(
        self,
        work_directory,
        axis_configuration,
        *,
        batch_size=Selection.DEFAULT_BATCH_SIZE,
        feature_metadata=None,
    ):
        if not 1 <= batch_size <= Selection.MAX_BATCH_SIZE:
            raise ValueError("batch size must be between 1 and 25,000")
        self.work = Path(work_directory)
        self.work.mkdir(parents=True, exist_ok=False)
        self.batch_size = batch_size
        self.axis_configuration = axis_configuration
        self.axes = axis_registry(axis_configuration)
        if feature_metadata is None:
            from tape_data_product.features import compact_product_schema as Schema

            feature_metadata = {f: Schema.field_metadata()[f] for f in FEATURES}
        if set(feature_metadata) != set(FEATURES):
            raise ValueError(
                "feature metadata must cover exactly the eighteen report fields"
            )
        self.feature_metadata = feature_metadata
        self.gate_counts = {
            h: np.zeros((3, 6), dtype=np.int64) for h in Selection.HORIZONS
        }
        self.feature_counts = {f: np.zeros((3, 3), dtype=np.int64) for f in FEATURES}
        self.participation_undefined = {
            h: np.zeros((3, 3), dtype=np.int64) for h in Selection.HORIZONS
        }
        self.status_counts = {
            h: {
                scope: np.zeros((3, len(MIDPOINT_STATUSES)), dtype=np.int64)
                for scope in ("post_discovery", "gate_passing")
            }
            for h in Selection.HORIZONS
        }
        self.status_support = {
            h: {
                scope: np.zeros((3, 2), dtype=np.int64)
                for scope in ("post_discovery", "gate_passing")
            }
            for h in Selection.HORIZONS
        }
        self.joints = {}
        for h in Selection.HORIZONS:
            for name, (x, y) in JOINT_STEMS.items():
                self.joints[(h, name)] = FixedJoint(self.axes[x], self.axes[y])
            for name, (x, y) in BANDED_JOINT_STEMS.items():
                self.joints[(h, name)] = FixedJoint(
                    self.axes[x], self.axes[y], banded=True
                )
        self.database = sqlite3.connect(self.work / "contributors.sqlite")
        self.database.execute("PRAGMA journal_mode=WAL")
        self.database.execute("PRAGMA cache_size=-8192")
        self.database.execute("PRAGMA temp_store=FILE")
        self.database.execute(
            "CREATE TABLE members (ordinal INTEGER PRIMARY KEY, partition_identity TEXT UNIQUE, date TEXT, symbol TEXT, source_json TEXT)"
        )
        self.database.execute(
            "CREATE TABLE contributions (population TEXT, horizon INTEGER, session INTEGER, symbol TEXT, date TEXT, partition_identity TEXT, n INTEGER, PRIMARY KEY(population,horizon,session,partition_identity)) WITHOUT ROWID"
        )
        self._last_member = None
        self._next_ordinal = 0
        self._extract_path = self.work / "gate_eligible_extract.parquet"
        self._extract_writer = None
        self._extract_paths = []
        self._extract_row_groups = 0
        self._rows = 0
        self._closed = False

    def _register_member(self, member):
        required = {"date", "symbol", "partition_identity", "source_identity"}
        if not required <= set(member):
            raise ValueError("member lacks date/symbol/partition/source identity")
        identity = member["partition_identity"]
        frozen = json.dumps(
            member["source_identity"],
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        record = (member["date"], member["symbol"], frozen)
        if self._last_member is not None and identity == self._last_member[0]:
            if self._last_member[1] != record:
                raise ValueError(
                    "partition identity reused with different member metadata"
                )
            return
        if self._next_ordinal >= MAX_MEMBERS:
            raise ValueError(
                "report member cap is 10,000; split the explicit release scope"
            )
        if self._extract_writer is not None:
            self._extract_writer.close()
            self._extract_writer = None
        self._extract_row_groups = 0
        self._extract_path = (
            self.work / f"gate_eligible_extract_{self._next_ordinal:06d}.parquet"
        )
        self.database.execute(
            "INSERT INTO members VALUES (?,?,?,?,?)",
            (self._next_ordinal, identity, member["date"], member["symbol"], frozen),
        )
        self._last_member = (identity, record)
        self._next_ordinal += 1

    def _add_contribution(self, population, horizon, mask, sessions, member):
        rows = []
        for session in range(3):
            n = int(np.count_nonzero(mask & (sessions == session)))
            if n:
                rows.append(
                    (
                        population,
                        horizon,
                        session,
                        member["symbol"],
                        member["date"],
                        member["partition_identity"],
                        n,
                    )
                )
        self.database.executemany(
            "INSERT INTO contributions VALUES (?,?,?,?,?,?,?) ON CONFLICT(population,horizon,session,partition_identity) DO UPDATE SET n=n+excluded.n",
            rows,
        )

    def _write_extract(self, values, feature_masks, sessions):
        import pyarrow as pa
        import pyarrow.parquet as pq

        schema = pa.schema(
            [pa.field("session", pa.int8(), nullable=False)]
            + [pa.field(f, pa.float64()) for f in FEATURES]
        )
        if self._extract_writer is None:
            self._extract_writer = pq.ParquetWriter(
                self._extract_path, schema, compression="zstd"
            )
            self._extract_paths.append(self._extract_path)
        keep = np.logical_or.reduce(list(feature_masks.values()))
        if not keep.any():
            return
        arrays = [pa.array(sessions[keep], type=pa.int8())]
        for feature in FEATURES:
            value = np.asarray(values[feature], dtype=np.float64)
            arrays.append(
                pa.array(
                    value[keep], mask=~feature_masks[feature][keep], type=pa.float64()
                )
            )
        if self._extract_row_groups >= MAX_EXTRACT_ROW_GROUPS:
            raise ValueError(
                "member extract exceeds 1,024 row groups; increase batch_size within its bound"
            )
        self._extract_writer.write_batch(
            pa.RecordBatch.from_arrays(arrays, schema=schema)
        )
        self._extract_row_groups += 1

    def add_batch(
        self,
        member,
        values,
        post_discovery,
        sessions,
        *,
        eligibility=None,
        reason_masks=None,
        midpoint_status=None,
    ):
        """Add one projected batch; all selection is performed on existing rows."""
        if self._closed:
            raise ValueError("aggregation is already closed")
        post = Selection._bool_array(post_discovery, "post_discovery")
        n = len(post)
        if n > self.batch_size or n > Selection.MAX_BATCH_SIZE:
            raise ValueError("projected batch exceeds configured bound")
        session = np.asarray(sessions)
        if (
            session.ndim != 1
            or len(session) != n
            or not np.all(np.isin(session, (0, 1, 2)))
        ):
            raise ValueError("invalid session batch")
        if not set(FEATURES) <= set(values):
            raise ValueError("batch lacks one or more report features")
        self._register_member(member)
        feature_masks = {}
        gates = {}
        for h in Selection.HORIZONS:
            gate = Selection.transaction_gate(values, post, h, eligibility)
            gates[h] = gate
            metrics = (
                gate.represented,
                gate.post_discovery,
                gate.input_eligible,
                gate.passing,
                gate.failing,
                gate.unavailable,
            )
            for s in range(3):
                self.gate_counts[h][s] += [
                    int(np.count_nonzero(mask & (session == s))) for mask in metrics
                ]
            self._add_contribution("gate_passing", h, gate.passing, session, member)
            for stem in FEATURE_STEMS:
                feature = Selection.feature_name(stem, h)
                good = Selection.feature_population(gate, values, feature, eligibility)
                feature_masks[feature] = good
                value = Selection._numeric_array(values[feature], feature, n)
                for s in range(3):
                    sm = session == s
                    self.feature_counts[feature][s] += (
                        int(np.count_nonzero(gate.passing & sm)),
                        int(np.count_nonzero(good & sm)),
                        int(np.count_nonzero(good & sm & (value == 0))),
                    )
                self._add_contribution("feature:" + feature, h, good, session, member)

            movement = Selection.feature_name("movement_mean_5s_bps", h)
            participation = Selection.feature_name("movement_participation", h)
            movement_good = feature_masks[movement]
            explicit = np.zeros(n, dtype=bool)
            evidence = np.zeros(n, dtype=bool)
            if reason_masks is not None and participation in reason_masks:
                reason = np.asarray(reason_masks[participation])
                if (
                    reason.ndim != 1
                    or len(reason) != n
                    or reason.dtype.kind not in "iu"
                ):
                    raise ValueError(
                        "participation reason mask must be an integer batch array"
                    )
                evidence[:] = True
                explicit = movement_good & ((reason & ZERO_TOTAL_MOVEMENT_REASON) != 0)
            for s in range(3):
                sm = session == s
                self.participation_undefined[h][s] += (
                    int(np.count_nonzero(movement_good & sm)),
                    int(np.count_nonzero(explicit & sm)),
                    int(np.count_nonzero(movement_good & sm & ~evidence)),
                )

            bands = Selection.rate_band_ids(gate, values)
            for name, (xstem, ystem) in JOINT_STEMS.items():
                xname, yname = Selection.feature_name(xstem, h), Selection.feature_name(
                    ystem, h
                )
                population = Selection.pair_population(
                    gate, values, xname, yname, eligibility
                )
                x = Selection._numeric_array(values[xname], xname, n)
                y = Selection._numeric_array(values[yname], yname, n)
                self.joints[(h, name)].add(x, y, session, population)
                self._add_contribution("pair:" + name, h, population, session, member)
            for name, (xstem, ystem) in BANDED_JOINT_STEMS.items():
                xname, yname = Selection.feature_name(xstem, h), Selection.feature_name(
                    ystem, h
                )
                population = Selection.pair_population(
                    gate, values, xname, yname, eligibility
                )
                x = Selection._numeric_array(values[xname], xname, n)
                y = Selection._numeric_array(values[yname], yname, n)
                self.joints[(h, name)].add(x, y, session, population, bands)
                for band, label in enumerate(Selection.RATE_BAND_LABELS):
                    self._add_contribution(
                        "pair:" + name + ":" + label,
                        h,
                        population & (bands == band),
                        session,
                        member,
                    )

            for s in range(3):
                sm = session == s
                if midpoint_status is None:
                    self.status_support[h]["post_discovery"][s, 1] += int(
                        np.count_nonzero(post & sm)
                    )
                    self.status_support[h]["gate_passing"][s, 1] += int(
                        np.count_nonzero(gate.passing & sm)
                    )
                    continue
                status = np.asarray(midpoint_status)
                if status.ndim != 1 or len(status) != n:
                    raise ValueError("midpoint status must match the batch length")
                if not np.all(np.isin(status, MIDPOINT_STATUSES)):
                    raise ValueError("unknown or null midpoint observation status")
                self.status_support[h]["post_discovery"][s, 0] += int(
                    np.count_nonzero(post & sm)
                )
                self.status_support[h]["gate_passing"][s, 0] += int(
                    np.count_nonzero(gate.passing & sm)
                )
                for j, name in enumerate(MIDPOINT_STATUSES):
                    self.status_counts[h]["post_discovery"][s, j] += int(
                        np.count_nonzero(post & sm & (status == name))
                    )
                    self.status_counts[h]["gate_passing"][s, j] += int(
                        np.count_nonzero(gate.passing & sm & (status == name))
                    )

        self._write_extract(values, feature_masks, session.astype(np.int8, copy=False))
        self._rows += n
        self.database.commit()

    def close(self):
        if self._extract_writer is not None:
            self._extract_writer.close()
            self._extract_writer = None
        self.database.commit()
        self._closed = True

    def _gate_coverage(self):
        names = (
            "represented",
            "post_discovery",
            "gate_eligible",
            "passing",
            "failing",
            "unavailable",
        )
        result = {}
        for h, counts in self.gate_counts.items():
            sessions = _session_and_pooled(counts, names)
            for values in sessions.values():
                if (
                    values["gate_eligible"] != values["passing"] + values["failing"]
                    or values["post_discovery"]
                    != values["gate_eligible"] + values["unavailable"]
                ):
                    raise ValueError("gate coverage does not reconcile")
                values["passing_stock_hours"] = values["passing"] / 3600
                values["retention_of_post_discovery"] = (
                    values["passing"] / values["post_discovery"]
                    if values["post_discovery"]
                    else None
                )
                values["retention_of_gate_eligible"] = (
                    values["passing"] / values["gate_eligible"]
                    if values["gate_eligible"]
                    else None
                )
            result[f"{h}s"] = sessions
        return result

    def _population_coverage(self):
        result = {"features": {}, "pairs": {}}
        names = ("gate_passing_baseline", "eligible", "zero")
        for feature, counts in self.feature_counts.items():
            sessions = _session_and_pooled(counts, names)
            for values in sessions.values():
                values["unavailable_within_gate"] = (
                    values["gate_passing_baseline"] - values["eligible"]
                )
            result["features"][feature] = dict(
                denominator_definition="same-horizon gate AND this feature eligibility",
                required=[feature, "same-horizon gate inputs"],
                unit=self.feature_metadata[feature]["unit"],
                sessions=sessions,
            )
        for (h, name), joint in self.joints.items():
            stems = (JOINT_STEMS | BANDED_JOINT_STEMS)[name]
            counts = joint.summary()
            gate_sessions = _session_and_pooled(
                self.gate_counts[h],
                (
                    "represented",
                    "post_discovery",
                    "gate_eligible",
                    "passing",
                    "failing",
                    "unavailable",
                ),
            )
            if joint.banded:
                pair_baseline = {}
                for session in (*Selection.SESSIONS, "pooled"):
                    eligible = sum(
                        counts[label][session]["eligible"]
                        for label in Selection.RATE_BAND_LABELS
                    )
                    pair_baseline[session] = dict(
                        gate_passing_baseline=gate_sessions[session]["passing"],
                        eligible=eligible,
                        unavailable_within_gate=gate_sessions[session]["passing"]
                        - eligible,
                    )
            else:
                for session in (*Selection.SESSIONS, "pooled"):
                    counts[session]["gate_passing_baseline"] = gate_sessions[session][
                        "passing"
                    ]
                    counts[session]["unavailable_within_gate"] = (
                        gate_sessions[session]["passing"] - counts[session]["eligible"]
                    )
                pair_baseline = counts
            payload = dict(
                denominator_definition="same-horizon gate AND eligibility of exactly the two plotted features; band shares divide by this pair baseline",
                required=[Selection.feature_name(stem, h) for stem in stems]
                + [
                    Selection.feature_name("trade_rate", h),
                    Selection.feature_name("trade_age_p90_seconds", h),
                ],
                x_feature=Selection.feature_name(stems[0], h),
                y_feature=Selection.feature_name(stems[1], h),
                x_unit=self.feature_metadata[Selection.feature_name(stems[0], h)][
                    "unit"
                ],
                y_unit=self.feature_metadata[Selection.feature_name(stems[1], h)][
                    "unit"
                ],
                x_edges=joint.x_edges.tolist(),
                y_edges=joint.y_edges.tolist(),
                axis_identity=Selection.canonical_digest(
                    dict(x=joint.x_edges.tolist(), y=joint.y_edges.tolist())
                ),
                counts=counts,
                pair_baseline=pair_baseline,
            )
            result["pairs"][f"{name}_{h}s"] = payload
        undefined_names = (
            "otherwise_movement_eligible",
            "zero_total_movement_undefined",
            "reason_evidence_missing",
        )
        result["participation_undefined"] = {
            f"{h}s": _session_and_pooled(counts, undefined_names)
            for h, counts in self.participation_undefined.items()
        }
        return result

    def _status_coverage(self):
        result = {}
        for h in Selection.HORIZONS:
            item = {
                scope: _session_and_pooled(counts, MIDPOINT_STATUSES)
                for scope, counts in self.status_counts[h].items()
            }
            for scope in ("post_discovery", "gate_passing"):
                support = _session_and_pooled(
                    self.status_support[h][scope],
                    ("support_rows_provided", "support_rows_missing"),
                )
                for session, counts in support.items():
                    item[scope][session].update(counts)
                    supplied = counts["support_rows_provided"]
                    if (
                        sum(item[scope][session][s] for s in MIDPOINT_STATUSES)
                        != supplied
                    ):
                        raise ValueError(
                            "midpoint status counts do not reconcile to projected support"
                        )
            item["source"] = (
                "explicit projected midpoint_age_observation_status; never inferred from rolling-age nulls"
            )
            item["rolling_p90_eligibility_location"] = "coverage/populations.json"
            result[f"{h}s"] = item
        return result

    def _concentration_group(self, population, horizon, session, expression):
        where = "population=? AND horizon=?"
        args = [population, horizon]
        if session != "pooled":
            where += " AND session=?"
            args.append(Selection.SESSIONS.index(session))
        rows = self.database.execute(
            f"SELECT SUM(n) AS c FROM contributions WHERE {where} GROUP BY {expression} ORDER BY c DESC",
            args,
        )
        total = squares = contributors = top = maximum = 0
        for (count,) in rows:
            count = int(count)
            if contributors == 0:
                maximum = count
            if contributors < 5:
                top += count
            contributors += 1
            total += count
            squares += count * count
        return dict(
            contributors=contributors,
            endpoints=total,
            maximum_share=maximum / total if total else None,
            top_five_share=top / total if total else None,
            herfindahl=squares / (total * total) if total else None,
        )

    def _concentration(self):
        populations = []
        for horizon in Selection.HORIZONS:
            names = [
                "gate_passing",
                *[
                    "feature:" + Selection.feature_name(stem, horizon)
                    for stem in FEATURE_STEMS
                ],
                *["pair:" + name for name in JOINT_STEMS],
            ]
            names.extend(
                "pair:" + name + ":" + label
                for name in BANDED_JOINT_STEMS
                for label in Selection.RATE_BAND_LABELS
            )
            populations.extend((name, horizon) for name in names)
        result = {}
        for population, horizon in populations:
            key = f"{population}@{horizon}s"
            result[key] = {}
            for session in (*Selection.SESSIONS, "pooled"):
                result[key][session] = {
                    "symbol": self._concentration_group(
                        population, horizon, session, "symbol"
                    ),
                    "symbol_day": self._concentration_group(
                        population, horizon, session, "partition_identity"
                    ),
                    "date": self._concentration_group(
                        population, horizon, session, "date"
                    ),
                }
        return result

    def _build_exact_tables(self, exact_directory):
        import duckdb
        from .ecdf import exact_table

        class ArrowReaderCompatibility:
            """Expose the reducer API across DuckDB Python package versions."""

            def __init__(self, connection):
                self.connection = connection

            def execute(self, query, parameters=None):
                (
                    self.connection.execute(query, parameters)
                    if parameters is not None
                    else self.connection.execute(query)
                )
                return self

            def to_arrow_reader(self, batch_size):
                method = getattr(self.connection, "to_arrow_reader", None)
                return (
                    method(batch_size)
                    if method is not None
                    else self.connection.fetch_record_batch(batch_size)
                )

        exact_directory.mkdir(parents=True)
        scratch = self.work / "exact_sort_scratch"
        scratch.mkdir(exist_ok=False)
        connection = duckdb.connect()
        connection.execute("SET memory_limit='256MB'")
        connection.execute("SET threads=1")
        connection.execute("SET max_temp_directory_size='1GB'")
        connection.execute("SET temp_directory=?", [str(scratch)])
        connection.execute("SET preserve_insertion_order=false")
        results = {}
        try:
            for h in Selection.HORIZONS:
                folder = exact_directory / f"{h}s"
                folder.mkdir()
                for stem in FEATURE_STEMS:
                    feature = Selection.feature_name(stem, h)
                    path = folder / (feature + ".parquet")
                    result = exact_table(
                        ArrowReaderCompatibility(connection),
                        self._extract_paths,
                        path,
                        batch_size=self.batch_size,
                        field=feature,
                    )
                    expected = _session_and_pooled(
                        self.feature_counts[feature], ("baseline", "eligible", "zero")
                    )
                    if result["totals"] != {
                        s: c["eligible"] for s, c in expected.items()
                    }:
                        raise ValueError(
                            "exact ECDF and feature availability disagree: " + feature
                        )
                    result.update(
                        path=str(path.relative_to(exact_directory.parent)),
                        sha256=Selection.file_sha256(path),
                        source_unit=self.feature_metadata[feature]["unit"],
                    )
                    results[feature] = result
        finally:
            connection.close()
        return results

    def _write_membership(self, path):
        rows = self.database.execute(
            "SELECT date,symbol,partition_identity,source_json FROM members ORDER BY ordinal"
        )
        count = 0
        with Path(path).open("x") as handle:
            for date, symbol, identity, source in rows:
                count += 1
                handle.write(
                    json.dumps(
                        dict(
                            date=date,
                            symbol=symbol,
                            partition_identity=identity,
                            source_identity=json.loads(source),
                        ),
                        sort_keys=True,
                        allow_nan=False,
                    )
                    + "\n"
                )
        return count

    def finalize(self, output_directory, binding):
        """Write a renderer-ready immutable bundle; the completion manifest is last."""
        Selection.validate_artifact_binding(binding)
        if (
            binding["bin_configuration"] != self.axis_configuration
            or binding["feature_definitions"] != self.feature_metadata
            or binding["units"]
            != {f: m["unit"] for f, m in self.feature_metadata.items()}
        ):
            raise ValueError("binding differs from actual axes/features")
        self.close()
        for h in Selection.HORIZONS:
            for base in ("movement_spread", "movement_participation"):
                if not np.array_equal(
                    self.joints[(h, base)].counts,
                    self.joints[(h, base + "_rate_bands")].counts.sum(axis=1),
                ):
                    raise ValueError(
                        "rate-band cells do not reconcile to pair baseline"
                    )
        if not self._extract_path.exists() and not getattr(
            self, "remote_extracts", False
        ):
            raise ValueError("cannot finalize an empty aggregation")
        output = Path(output_directory)
        output.mkdir(parents=True, exist_ok=False)
        coverage_dir = output / "coverage"
        coverage_dir.mkdir()
        joints_dir = output / "joints"
        joints_dir.mkdir()
        membership_count = self._write_membership(output / "membership.jsonl")
        gate = self._gate_coverage()
        populations = self._population_coverage()
        status = self._status_coverage()
        concentration = self._concentration()
        _write_json(coverage_dir / "gate.json", gate)
        _write_json(coverage_dir / "populations.json", populations)
        _write_json(coverage_dir / "midpoint_status.json", status)
        _write_json(coverage_dir / "concentration.json", concentration)
        joint_receipts = {}
        for (h, name), joint in self.joints.items():
            folder = joints_dir / f"{h}s"
            folder.mkdir(exist_ok=True)
            array_path = folder / (name + ".npz")
            np.savez_compressed(
                array_path,
                counts=joint.counts,
                x_edges=joint.x_edges,
                y_edges=joint.y_edges,
            )
            stems = (JOINT_STEMS | BANDED_JOINT_STEMS)[name]
            population_note = populations["pairs"][f"{name}_{h}s"]
            note = dict(
                name=name,
                horizon_seconds=h,
                x_feature=Selection.feature_name(stems[0], h),
                y_feature=Selection.feature_name(stems[1], h),
                x_unit=population_note["x_unit"],
                y_unit=population_note["y_unit"],
                x_edges=joint.x_edges.tolist(),
                y_edges=joint.y_edges.tolist(),
                axis_identity=population_note["axis_identity"],
                counts_shape=list(joint.counts.shape),
                band_labels=list(Selection.RATE_BAND_LABELS) if joint.banded else None,
                coverage=joint.summary(),
                pair_baseline=population_note["pair_baseline"],
                array_sha256=Selection.file_sha256(array_path),
                normalization="each panel uses its own eligible integer count; off-axis remains in denominator",
                bin_semantics="0=exact zero; 1=positive underflow; 2..J=finite intervals; J+1=overflow; final finite edge included",
            )
            _write_json(folder / (name + ".json"), note)
            joint_receipts[f"{name}_{h}s"] = note
        exact = self._build_exact_tables(output / "exact")
        files = {}
        for path in sorted(p for p in output.rglob("*") if p.is_file()):
            files[str(path.relative_to(output))] = dict(
                sha256=Selection.file_sha256(path), bytes=path.stat().st_size
            )
        manifest = dict(
            state="complete",
            schema=getattr(self, "bundle_schema", "tape_report_numerical_artifacts_v1"),
            binding=binding,
            artifact_identity=binding["artifact_identity"],
            rows=self._rows,
            members=membership_count,
            membership_sha256=files["membership.jsonl"]["sha256"],
            exact_tables=exact,
            joint_arrays={
                k: {
                    "array_sha256": v["array_sha256"],
                    "counts_shape": v["counts_shape"],
                }
                for k, v in joint_receipts.items()
            },
            files=files,
            resource_contract=dict(
                batch_size=self.batch_size,
                maximum_batch_size=Selection.MAX_BATCH_SIZE,
                maximum_members=MAX_MEMBERS,
                maximum_extract_row_groups_per_member=MAX_EXTRACT_ROW_GROUPS,
                duckdb_memory_limit="256MB",
                duckdb_threads=1,
                maximum_sort_spill="1GB",
                complexity="scan O(NF+NP), exact sort O(F*N*log(N)), RAM O(BF+P*J^2), retained disk O(NF)",
            ),
            matched_endpoints=False,
        )
        _write_json(output / "manifest.json", manifest)
        return manifest


def validate_bundle(path, expected_binding):
    """Fail closed on changed inputs/policy/config/code or artifact bytes."""
    path = Path(path)
    manifest = json.loads((path / "manifest.json").read_text())
    Selection.validate_artifact_binding(expected_binding)
    if (
        manifest.get("schema")
        not in (
            "tape_report_numerical_artifacts_v1",
            "tape_report_numerical_artifacts_v2",
        )
        or manifest.get("artifact_identity") != expected_binding["artifact_identity"]
        or manifest.get("state") != "complete"
        or manifest.get("binding") != expected_binding
    ):
        raise ValueError("report bundle binding mismatch")
    required = {
        "membership.jsonl",
        *(
            "coverage/" + n + ".json"
            for n in ("gate", "populations", "midpoint_status", "concentration")
        ),
    }
    if manifest["schema"] == "tape_report_numerical_artifacts_v1":
        required.update(
            f"exact/{h}s/{stem}_{h}s.parquet"
            for h in Selection.HORIZONS
            for stem in FEATURE_STEMS
        )
    else:
        required.update(
            f"exact/{h}s/{stem}_{h}s.json"
            for h in Selection.HORIZONS
            for stem in FEATURE_STEMS
        )
    required.update(
        f"joints/{h}s/{name}.{ext}"
        for h in Selection.HORIZONS
        for name in (*JOINT_STEMS, *BANDED_JOINT_STEMS)
        for ext in ("npz", "json")
    )
    if not required <= set(manifest.get("files", {})):
        raise ValueError("report bundle required artifact missing")
    for name, identity in manifest["files"].items():
        if Path(name).is_absolute() or ".." in Path(name).parts:
            raise ValueError("invalid artifact path")
        artifact = path / name
        if (
            not artifact.is_file()
            or artifact.stat().st_size != identity["bytes"]
            or Selection.file_sha256(artifact) != identity["sha256"]
        ):
            raise ValueError("report bundle artifact changed: " + name)
    return manifest
