from __future__ import annotations

import importlib.util
import json
import math
from pathlib import Path
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/"src/04_research"))
import economic_tape_neighbors_v3 as NN
import run_economic_tape_v3 as RUN
V = NN.V3


def quote(ts, seq=0, bid=99.95, ask=100.05, size=10., conditions=None, exchange=1):
    return dict(sip_timestamp=ts, sequence_number=seq, bid_price=bid, ask_price=ask,
                bid_size=size, ask_size=size+1, bid_exchange=exchange, ask_exchange=1,
                participant_timestamp=ts, conditions=conditions or [], indicators=[], tape=1, trf_timestamp=None)


def trade(ts, seq=0, price=100., size=1., conditions=None, latency=0, correction=0):
    return dict(sip_timestamp=ts, sequence_number=seq, participant_timestamp=ts-latency,
                price=price, size=size, decimal_size=None, conditions=conditions or [], correction=correction)


def raw_pair(path, day, quotes, trades):
    path.mkdir(exist_ok=True, parents=True)
    for name, rows, factory in (("quotes", quotes, quote), ("trades", trades, trade)):
        schema = pa.Table.from_pylist([factory(V.session_bounds(day)[0])]).schema
        # Empty list/null columns must remain usable with real condition arrays.
        for col in ("conditions", "indicators"):
            if col in schema.names:
                schema = schema.set(schema.get_field_index(col), pa.field(col, pa.list_(pa.int64())))
        if "decimal_size" in schema.names:
            schema = schema.set(schema.get_field_index("decimal_size"), pa.field("decimal_size", pa.float64()))
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path/f"{name}.parquet")
    return path/"quotes.parquet", path/"trades.parquet"


def primitive(i=0, day=NN.REFERENCE_DATE, **changes):
    p = dict(session_date=day, symbol="TEST", interval_end_ns=V.session_bounds(day)[0]+(i+1)*V.NS,
             continuity_segment_id=0, generation=0, halt_interval_active=False, halt_interval_id=None,
             halt_resume_boundary=False, historical_ex_post_overlay=True,
             quote_source_file_accepted=True, trade_source_file_accepted=True,
             midpoint=100*math.exp(i*.00001), midpoint_duration=1., quote_age=100., trade_age=200.,
             trade_count=1., dollars=100., depth_duration=1., bid_mass=1000., ask_mass=1100.,
             unlocked_duration=1., spread_mass=10., changes=1., price_changes=1., size_changes=0., raw_messages=2.)
    p.update(changes)
    return p


def test_joint_spread_invalid_locks_and_explicit_locks(tmp_path):
    day = NN.REFERENCE_DATE
    start, _ = V.session_bounds(day)
    # 95% valid unlocked / 5% invalid locked: mean 10bps and coverage .95, not 10.555/.90.
    quotes = [quote(start-1)]
    for s in range(60):
        quotes.extend([quote(start+s*V.NS), quote(start+s*V.NS+950_000_000, bid=100., ask=100., conditions=[20])])
    q, t = raw_pair(tmp_path, day, quotes, [trade(start)])
    model = V.FeatureStream()
    for p in V.primitives(q, t, day, "TEST", seconds=60, batch_size=7):
        out = model.push(p)
    assert out["quoted_spread_bps_60s"] == pytest.approx(10.)
    assert out["quoted_spread_valid_fraction_60s"] == pytest.approx(.95)
    q, t = raw_pair(tmp_path/"explicit", day, [quote(start-1, conditions=[85])], [])
    p = next(V.primitives(q, t, day, "TEST", seconds=1))
    assert p["unlocked_duration"] == p["spread_mass"] == 0
    assert p["midpoint_duration"] == 1


def test_exact_duration_thresholds_do_not_fail_from_subsecond_roundoff(tmp_path):
    day = NN.REFERENCE_DATE
    start, _ = V.session_bounds(day)
    rows = []
    for second in range(300):
        for phase in range(10):
            # Nine unlocked 100ms states; only eight have usable displayed size.
            rows.append(quote(start+second*V.NS+phase*100_000_000,
                              bid=99.95 if phase < 9 else 100., ask=100.05 if phase < 9 else 100.,
                              size=10.+phase if phase < 8 else 0.))
    q,t=raw_pair(tmp_path,day,rows,[])
    model=V.FeatureStream()
    for p in V.primitives(q,t,day,"TEST",seconds=300,batch_size=17):
        assert p["unlocked_duration_ns"] == 900_000_000
        assert p["depth_duration_ns"] == 800_000_000
        row=model.push(p)
    for h in (60,300):
        assert row[f"quoted_spread_valid_fraction_{h}s"] == .9
        assert row[f"quoted_spread_valid_{h}s"]
        assert row[f"displayed_notional_valid_fraction_{h}s"] == .8
        assert row[f"displayed_notional_support_valid_{h}s"]


def test_churn_warmup_refresh_exchange_invalid_and_batch_ties(tmp_path):
    day = NN.REFERENCE_DATE
    start, _ = V.session_bounds(day)
    quotes = [quote(start-1), quote(start), quote(start+1, seq=1, size=12),
              quote(start+1, seq=2, size=12, exchange=2),
              quote(start+2, size=12, exchange=2, conditions=[20]), quote(start+V.NS)]
    q, t = raw_pair(tmp_path, day, quotes, [trade(start+V.NS)])
    rows = list(V.primitives(q, t, day, "TEST", seconds=2, batch_size=1))
    assert rows[0]["changes"] == 3
    assert rows[0]["price_changes"] == 0
    assert rows[0]["size_changes"] == 1
    assert rows[0]["raw_messages"] == 4
    assert math.isnan(rows[0]["quote_age"])
    assert rows[0]["trade_count"] == 0 and math.isnan(rows[0]["trade_age"])
    assert rows[1]["trade_count"] == 1 and rows[1]["trade_age"] == 1000
    other = list(V.primitives(q, t, day, "TEST", seconds=2, batch_size=25_000))
    for a, b in zip(rows, other):
        for key in a:
            if isinstance(a[key], float) and math.isnan(a[key]):
                assert math.isnan(b[key])
            else:
                assert a[key] == b[key]


def test_no_warmup_first_quote_initializes_zero_and_duplicate_fails(tmp_path):
    day = NN.REFERENCE_DATE
    start, _ = V.session_bounds(day)
    q, t = raw_pair(tmp_path, day, [quote(start)], [])
    p = next(V.primitives(q, t, day, "TEST", seconds=1))
    assert p["changes"] == 0 and p["raw_messages"] == 1
    q, t = raw_pair(tmp_path/"dupe", day, [quote(start), quote(start)], [])
    with pytest.raises(ValueError, match="duplicate"):
        list(V.primitives(q, t, day, "TEST", seconds=1, batch_size=1))


def test_maturity_counts_halt_carry_and_boundary():
    model = V.FeatureStream()
    for i in range(301):
        row = model.push(primitive(i))
    assert row["movement_valid_5s_count_60s"] == 56
    assert row["movement_valid_5s_count_300s"] == 296
    assert row["pilot_base_comparison_eligible"]
    row = model.push(primitive(301, halt_interval_active=True, halt_interval_id="H"))
    assert row["state_contains_pre_halt_history_300s"]
    assert row["state_observed_second_count_300s"] == 300
    assert math.isnan(row["quoted_spread_bps_300s"])
    for i in range(302, 602):
        row = model.push(primitive(i, generation=1, halt_resume_boundary=i == 302))
    assert row["quoted_spread_valid_300s"]
    assert not row["state_fully_post_halt_300s"]
    row = model.push(primitive(602, generation=1))
    assert row["state_fully_post_halt_300s"] and row["pilot_base_comparison_eligible"]


def test_halt_source_break_preserves_history_and_resets_raw_origins(tmp_path):
    day = NN.REFERENCE_DATE
    start, _ = V.session_bounds(day)
    q, t = raw_pair(tmp_path, day, [quote(start-1), quote(start+2*V.NS), quote(start+4*V.NS)], [trade(start)])
    rows = list(V.primitives(q, t, day, "TEST", seconds=6,
                            halts=[(start+V.NS, start+3*V.NS, "H")],
                            continuity_breaks_ns=[start+2*V.NS]))
    assert all(row["continuity_segment_id"] == 0 for row in rows)
    assert math.isnan(rows[3]["quote_age"]) and math.isnan(rows[3]["trade_age"])
    assert rows[4]["changes"] == 0
    model = V.FeatureStream()
    out = [model.push(row) for row in rows]
    assert out[-1]["state_observed_second_count_60s"] == 4


def test_session_starts_halted_without_inventing_history(tmp_path):
    model = V.FeatureStream()
    for i in range(2):
        row = model.push(primitive(i, halt_interval_active=True, halt_interval_id="H"))
        assert row["state_observed_second_count_300s"] == 0
        assert not row["state_carried_forward_during_halt_300s"]
        assert not row["state_contains_pre_halt_history_300s"]
    row = model.push(primitive(2, generation=1, halt_resume_boundary=True))
    assert row["state_observed_second_count_300s"] == 1
    day = NN.REFERENCE_DATE
    start, _ = V.session_bounds(day)
    q, t = raw_pair(tmp_path, day, [quote(start-1),quote(start+1_750_000_000),quote(start+2*V.NS)], [])
    rows = list(V.primitives(q,t,day,"TEST",seconds=3,halts=[(start+500_000_000,start+1_500_000_000,"H")]))
    assert [r["halt_interval_active"] for r in rows] == [True,True,False]
    assert rows[2]["raw_messages"] == 1 and rows[2]["changes"] == 0
    assert rows[2]["quote_age"] == 1000


def test_zero_sums_and_all_locked_window_are_exact():
    w = V.RollingValues(60)
    for value in np.random.default_rng(4).lognormal(18, 5, 60):
        w.append(value)
    for _ in range(60):
        w.append(0)
    assert w.total == w.mean() == 0
    model = V.FeatureStream()
    for i in range(301):
        row = model.push(primitive(i, unlocked_duration=.951, spread_mass=9.51))
    for i in range(301, 601):
        row = model.push(primitive(i, trade_count=0, dollars=0, unlocked_duration=0, spread_mass=0))
    assert row["trade_rate_300s"] == row["dollar_rate_300s"] == 0
    assert row["quoted_spread_valid_fraction_300s"] == 0
    assert math.isnan(row["quoted_spread_bps_300s"])


def test_support_not_numeric_gate_and_no_partial_distance():
    m = V.FeatureStream()
    for i in range(301):
        row = m.push(primitive(i, unlocked_duration=.89, spread_mass=8.9))
    assert row["quoted_spread_bps_300s"] == pytest.approx(10)
    assert not row["quoted_spread_valid_300s"]
    assert not row["pilot_base_comparison_eligible"]
    row = m.push(primitive(301, quote_source_file_accepted=False))
    assert math.isnan(row["quoted_spread_valid_fraction_60s"])


def test_raw_trade_eligibility_form_t_fractional_latency(tmp_path):
    day = NN.REFERENCE_DATE
    start, _ = V.session_bounds(day)
    trades = [trade(start, conditions=[12], size=.5), trade(start+1, conditions=[13]),
              trade(start+2, latency=V.NS+1), trade(start+3, latency=-1)]
    q, t = raw_pair(tmp_path, day, [], trades)
    row = next(V.primitives(q, t, day, "TEST", seconds=1))
    assert row["trade_count"] == 1 and row["dollars"] == 50
    rth = start+19800*V.NS
    _, t = raw_pair(tmp_path/"rth", day, [], [trade(rth, conditions=[12])])
    assert not next(V.events(t, "trade", day))[1]["eligible"]


def panel(path, day, values):
    rows = []
    for i, x in enumerate(values):
        row = dict(session_date=day, symbol="TEST", interval_end_ns=V.session_bounds(day)[0]+(i+1)*300*V.NS,
                   pilot_base_comparison_eligible=True)
        row.update(zip(V.FEATURES, x))
        rows.append(row)
    table = pa.Table.from_pylist(rows).replace_schema_metadata({b"v3_identity": json.dumps(V.contract_identity()).encode()})
    pq.write_table(table, path)


def test_scaler_linear_log_interpolation_date_isolation_and_neighbors(tmp_path, monkeypatch):
    x = np.random.default_rng(1).uniform(.1, 100, (11, 18))
    q = np.random.default_rng(2).uniform(.1, 100, (3, 18))
    ref, query = tmp_path/"ref.parquet", tmp_path/"query.parquet"
    panel(ref, NN.REFERENCE_DATE, x)
    panel(query, NN.QUERY_DATE, q)
    monkeypatch.setattr(NN, "REFERENCE_BLOCK", 3)
    result = NN.run([ref], [query], tmp_path/"run", k=5)
    scaler = json.loads((tmp_path/"run/scaler.json").read_text())
    assert scaler["median"] == pytest.approx(np.quantile(np.log1p(x), .5, axis=0))
    assert scaler["iqr"] == pytest.approx(np.quantile(np.log1p(x), .75, axis=0)-np.quantile(np.log1p(x), .25, axis=0))
    for line in (tmp_path/"run/matches.jsonl").read_text().splitlines():
        row = json.loads(line)
        idx = (row["query_interval_end_ns"]-V.session_bounds(NN.QUERY_DATE)[0])//(300*V.NS)-1
        brute = np.sum((NN.transform(x, scaler)-NN.transform(q[idx], scaler))**2*V.WEIGHTS, axis=1)
        order = np.argsort(brute, kind="stable")
        expected = V.session_bounds(NN.REFERENCE_DATE)[0]+(order[row["rank"]-1]+1)*300*V.NS
        assert row["reference_interval_end_ns"] == expected
        assert sum(row["weighted_squared_contributions"].values()) == pytest.approx(row["D_base"]**2)
        assert sum(d*d for d in row["family_distances"].values())/6 == pytest.approx(row["D_base"]**2)
    panel(query, NN.QUERY_DATE, q*1000)
    NN.run([ref], [query], tmp_path/"run2", k=5)
    assert json.loads((tmp_path/"run2/scaler.json").read_text())["iqr"] == scaler["iqr"]
    assert result["query_count"] == 3


def test_scaler_zero_iqr_wrong_date_and_duplicate_sources_fail(tmp_path):
    panel(tmp_path/"constant", NN.REFERENCE_DATE, np.ones((3,18)))
    panel(tmp_path/"query", NN.QUERY_DATE, np.ones((3,18)))
    with pytest.raises(ValueError, match="IQR"):
        NN.run([tmp_path/"constant"], [tmp_path/"query"], tmp_path/"fail")
    with pytest.raises(ValueError, match="wrong date"):
        list(NN.feature_rows([tmp_path/"query"], NN.REFERENCE_DATE))
    db = NN.connect(tmp_path/"db")
    with pytest.raises(NN.sqlite3.IntegrityError):
        NN.load_population(db, "reference", [tmp_path/"constant"]*2, NN.REFERENCE_DATE, V.contract_identity()["contract_hash"])
    db.close()


def test_bid_ask_separate_and_stable_distance_ties(tmp_path):
    scaler = {"median": [0]*18, "iqr": [1]*18}
    a = np.ones(18)
    a[-4:] = [10,10,100,100]
    b = np.ones(18)
    b[-4:] = [100,100,10,10]
    distance, family, _ = NN.decompose(a,b,scaler)
    assert distance > 0 and family["displayed_capacity"] > 0
    assert all(d == 0 for name, d in family.items() if name != "displayed_capacity")
    db = NN.connect(tmp_path/"db")
    panel(tmp_path/"ref", NN.REFERENCE_DATE, [a,a,a])
    NN.load_population(db,"reference",[tmp_path/"ref"],NN.REFERENCE_DATE,V.contract_identity()["contract_hash"])
    query = ("TEST", 0, "", *a)
    matches = list(NN.nearest(db,[query],scaler,k=2))[0][1]
    assert [x[2] for x in matches] == [V.session_bounds(NN.REFERENCE_DATE)[0]+i*300*V.NS for i in (1,2)]
    db.close()


def test_atomic_feature_failure_and_native_nulls(tmp_path):
    day = NN.REFERENCE_DATE
    start, _ = V.session_bounds(day)
    q,t = raw_pair(tmp_path,day,[quote(start)],[])
    output = tmp_path/"feature.parquet"
    V.write_features(q,t,day,"TEST",output,seconds=2)
    table = pq.read_table(output)
    assert table["trade_age_p90_60s"].null_count == 2
    assert table["quoted_spread_bps_60s"].null_count == 2
    with pytest.raises(FileExistsError):
        V.write_features(q,t,day,"TEST",output,seconds=2)
    q,t = raw_pair(tmp_path/"bad",day,[quote(start),quote(start)],[])
    with pytest.raises(ValueError):
        V.write_features(q,t,day,"TEST",tmp_path/"failed.parquet",seconds=2)
    assert not (tmp_path/"failed.parquet").exists()
    assert not (tmp_path/"failed.partial").exists()


def test_disk_preflight_refuses_insufficient_space(tmp_path, monkeypatch):
    monkeypatch.setattr(RUN.shutil,"disk_usage",lambda _: type("Usage",(),{"free":10})())
    with pytest.raises(OSError, match="insufficient scratch"):
        RUN.require_space(tmp_path,11)


def test_runner_binds_source_provenance_and_requires_full_checkpoint(tmp_path, monkeypatch):
    from types import SimpleNamespace
    day = NN.REFERENCE_DATE
    start, _ = V.session_bounds(day)
    raw_pair(tmp_path/"raw",day,[quote(start-1)],[])
    source = dict(session_date=day,symbol="TEST",local_dir=str(tmp_path/"raw"),registry_dir="test-registry",
                  coverage={"trades":"[04:00,20:00) ET","quotes":"[03:55,20:00) ET"},
                  coverage_provenance="synthetic fixture",halt_coverage_dates=[day],
                  halt_coverage_provenance="synthetic fixture",continuity_breaks_ns=[])
    plan = tmp_path/"plan.json"
    plan.write_text(json.dumps({"universe_provenance":"synthetic test","sources":[source]}))
    args = SimpleNamespace(plan=plan,output=tmp_path/"out",scratch_root=tmp_path/"scratch",
                           sample_seconds=None,full_run_approved=False,batch_size=2,features_only=True)
    with pytest.raises(ValueError,match="confirmation"):
        RUN.worker(args)
    args.sample_seconds=2
    monkeypatch.setattr(RUN,"load_halts",lambda *args: ([],{"test_registry":True}))
    RUN.worker(args)
    meta=json.loads(pq.ParquetFile(args.output/f"{day}_TEST.parquet").schema_arrow.metadata[b"v3_identity"])
    assert meta["raw_sources"]["quotes"]["rows"] == 1
    assert meta["source_acceptance"] == "prefix-only"
    assert meta["continuity_breaks_ns"] == []
    assert meta["halt_coverage_dates"] == [day]
    assert json.loads((args.output/"run_manifest.json").read_text())["status"] == "bounded_sample_complete"
