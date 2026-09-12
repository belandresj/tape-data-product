"""Small offline boundary, coverage and immutable-storage tests."""
from io import BytesIO
import json
from pathlib import Path
import pytest
from tape_data_product.acquisition.common import bounds, MINUTE_NS, dump, digest
from tape_data_product.acquisition.screen import screen
from tape_data_product.acquisition.vendor import FixtureClient, acquire_reference, acquire_minutes, normalize_stream
from tape_data_product.storage.catalog import verify_pair, publish_pair, stage_pair

DAY = '2026-06-18'


@pytest.fixture(autouse=True)
def bounded_fake_disk(monkeypatch):
    from types import SimpleNamespace
    monkeypatch.setattr('tape_data_product.storage.catalog.shutil.disk_usage', lambda path: SimpleNamespace(free=10 * 1024 ** 3))


def jsonl(path, rows):
    path.write_text(''.join(json.dumps(row) + '\n' for row in rows))
    return path


def bar(symbol, ts, tr=800, **changes):
    return dict(symbol=symbol, window_start=ts, transactions=tr, volume=100, open=10, close=10, high=11, low=10, **changes)


def test_screen_consecutive_denominators_and_first_endpoint(tmp_path):
    lo, _ = bounds(DAY)
    ref = jsonl(tmp_path / 'ref.jsonl', [dict(session_date=DAY, symbol=s, type='CS') for s in ['PASS', 'GAP', 'ABSENT', 'INVALID', 'FLOOR']])
    rows = [bar('PASS', lo), bar('PASS', lo + MINUTE_NS), bar('PASS', lo + 2 * MINUTE_NS),
            bar('GAP', lo), bar('GAP', lo + 2 * MINUTE_NS),
            {**bar('INVALID', lo), 'high': 9}, bar('INVALID', lo + MINUTE_NS),
            bar('FLOOR', lo, tr=99), bar('FLOOR', lo + MINUTE_NS, tr=1501)]
    minutes = jsonl(tmp_path / 'minutes.jsonl', reversed(rows))
    selection = screen(ref, minutes, DAY, tmp_path / 'screen')
    selected = [json.loads(x) for x in selection.read_text().splitlines()]
    assert [r['symbol'] for r in selected] == ['PASS']
    assert selected[0]['discovery_endpoint_ns'] == lo + 2 * MINUTE_NS
    assert 'received_at_ns' not in selected[0]
    counts = json.loads((selection.parent / 'denominators.json').read_text())
    assert counts['reference_symbols'] == 5
    assert counts['eligible_source_symbols'] == 4
    assert counts['invalid_ohlc_rows'] == 1
    assert counts['qualifying_windows'] == 2


def test_screen_duplicate_minute_fails(tmp_path):
    lo, _ = bounds(DAY)
    ref = jsonl(tmp_path / 'ref.jsonl', [dict(session_date=DAY, symbol='A', type='CS')])
    minutes = jsonl(tmp_path / 'minutes.jsonl', [bar('A', lo), bar('A', lo)])
    with pytest.raises(Exception, match='UNIQUE'):
        screen(ref, minutes, DAY, tmp_path / 'screen')


def test_reference_and_minute_producers(tmp_path):
    ref = acquire_reference(FixtureClient([{'status': 'OK', 'results': [{'ticker': 'A', 'type': 'CS'}]}]), DAY, tmp_path / 'reference')
    assert json.loads(ref.read_text())['session_date'] == DAY
    lo, _ = bounds(DAY)
    result = acquire_minutes(FixtureClient([{'status': 'OK', 'results': [{'t': lo // 1_000_000, 'n': 800, 'v': 100, 'o': 10, 'c': 10, 'h': 11, 'l': 10}]}]), DAY, 'A', tmp_path / 'minutes')
    assert json.loads(result.read_text())['window_start'] == lo


def trade(ts, sequence=1):
    return dict(sip_timestamp=ts, sequence_number=sequence, price=10., size=100.)


def test_pagination_failure_preserves_no_completed_file(tmp_path):
    lo, _ = bounds(DAY)
    url = 'https://api.massive.com/v3/trades/A?cursor=x'
    client = FixtureClient([{'status': 'OK', 'results': [trade(lo)], 'next_url': url},
                            {'status': 'OK', 'results': [trade(lo)], 'next_url': url}])
    path = tmp_path / 'trades.parquet'
    with pytest.raises(ValueError, match='Repeated'):
        normalize_stream(client, DAY, 'A', 'trades', path)
    assert not path.exists()
    assert not path.with_suffix('.receipt.json').exists()


def test_out_of_order_cross_page_rejected(tmp_path):
    lo, _ = bounds(DAY)
    client = FixtureClient([{'status': 'OK', 'results': [trade(lo + 1)], 'next_url': 'https://api.massive.com/v3/trades/A?cursor=next'},
                            {'status': 'OK', 'results': [trade(lo)]}])
    with pytest.raises(ValueError, match='out-of-order'):
        normalize_stream(client, DAY, 'A', 'trades', tmp_path / 'trades.parquet')


def make_pair(root):
    root.mkdir()
    streams = {}
    for stream in ('trades', 'quotes'):
        lo, _ = bounds(DAY, stream)
        row = trade(lo) if stream == 'trades' else dict(sip_timestamp=lo, bid_price=10., ask_price=10.01, bid_size=1., ask_size=1.)
        streams[stream] = normalize_stream(FixtureClient([{'status': 'OK', 'results': [row]}]), DAY, 'A', stream, root / f'{stream}.parquet')
    dump(root / 'pair.json', dict(version='canonical_tq_pair_v1', session_date=DAY, symbol='A', streams=streams, selection_sha256='test', synthetic=False))
    return root


class FakeError(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class FakeR2:
    def __init__(self):
        self.objects = {}
        self.puts = 0

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise FakeError('404')
        data, metadata = self.objects[Key]
        return {'ContentLength': len(data), 'Metadata': metadata.copy()}

    def put_object(self, Bucket, Key, Body, ContentLength, Metadata, ContentType, IfNoneMatch):
        assert IfNoneMatch == '*'
        if Key in self.objects:
            raise FakeError('412')
        data = Body.read()
        assert len(data) == ContentLength
        self.objects[Key] = data, Metadata.copy()
        self.puts += 1

    def get_object(self, Bucket, Key):
        return {'Body': BytesIO(self.objects[Key][0])}


def test_immutable_publication_and_verified_stage(tmp_path):
    pair = make_pair(tmp_path / 'pair')
    client = FakeR2()
    assert set(publish_pair(client, 'bucket', pair).values()) == {'uploaded_verified'}
    assert set(publish_pair(client, 'bucket', pair).values()) == {'existing_identical'}
    assert client.puts == 2
    staged = stage_pair(client, 'bucket', pair / 'pair.json', tmp_path / 'staged')
    assert verify_pair(staged)['symbol'] == 'A'
    key = next(iter(client.objects))
    data, metadata = client.objects[key]
    client.objects[key] = data, {**metadata, 'requested-start-ns': '0'}
    with pytest.raises(ValueError, match='conflict'):
        publish_pair(client, 'bucket', pair)
    assert client.puts == 2


def test_corrupt_body_rejected_even_when_head_matches(tmp_path):
    pair = make_pair(tmp_path / 'pair')
    client = FakeR2()
    publish_pair(client, 'bucket', pair)
    key = next(iter(client.objects))
    data, metadata = client.objects[key]
    client.objects[key] = b'x' + data[1:], metadata
    with pytest.raises(ValueError, match='byte identity'):
        stage_pair(client, 'bucket', pair / 'pair.json', tmp_path / 'bad')


def test_synthetic_coverage_cannot_publish(tmp_path):
    pair = make_pair(tmp_path / 'pair')
    path = pair / 'pair.json'
    manifest = json.loads(path.read_text())
    manifest['synthetic'] = True
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='Synthetic'):
        publish_pair(FakeR2(), 'bucket', pair)


def test_coverage_requires_terminal_receipts_and_exact_source_bytes(tmp_path):
    reference = acquire_reference(FixtureClient([{'status': 'OK', 'results': [{'ticker': 'A', 'type': 'CS'}]}]), DAY, tmp_path / 'reference')
    minutes = acquire_minutes(FixtureClient([{'status': 'OK', 'results': []}]), DAY, 'A', tmp_path / 'minutes')
    screen(reference, minutes, DAY, tmp_path / 'verified', minute_receipts=[minutes.parent])
    counts = json.loads((tmp_path / 'verified/denominators.json').read_text())
    assert counts['population_coverage_complete'] is True
    assert counts['reference_symbols'] == 1 and counts['eligible_source_symbols'] == 0
    unrelated = jsonl(tmp_path / 'unrelated.jsonl', [bar('A', bounds(DAY)[0])])
    with pytest.raises(ValueError, match='differs from concatenated'):
        screen(reference, unrelated, DAY, tmp_path / 'bad', minute_receipts=[minutes.parent])


def test_storage_reserve_and_existing_corruption_fail(tmp_path, monkeypatch):
    from types import SimpleNamespace
    pair = make_pair(tmp_path / 'pair')
    client = FakeR2()
    publish_pair(client, 'bucket', pair)
    monkeypatch.setattr('tape_data_product.storage.catalog.shutil.disk_usage', lambda path: SimpleNamespace(free=3 * 1024 ** 3))
    with pytest.raises(ValueError, match='reserve'):
        stage_pair(client, 'bucket', pair / 'pair.json', tmp_path / 'staged')
    key = next(iter(client.objects))
    data, metadata = client.objects[key]
    client.objects[key] = b'x' + data[1:], metadata
    with pytest.raises(ValueError, match='byte identity'):
        publish_pair(client, 'bucket', pair)
    assert client.puts == 2


def test_detached_reference_cannot_claim_population_complete(tmp_path):
    """A truncated reference plus terminal minutes proves only that subset."""
    reference = jsonl(
        tmp_path / "reference.jsonl",
        [dict(session_date=DAY, symbol="A", type="CS")],
    )
    minutes = acquire_minutes(
        FixtureClient([{"status": "OK", "results": []}]),
        DAY,
        "A",
        tmp_path / "minutes",
    )
    screen(
        reference,
        minutes,
        DAY,
        tmp_path / "screen",
        minute_receipts=[minutes.parent],
    )
    counts = json.loads((tmp_path / "screen/denominators.json").read_text())
    assert counts["missing_minute_sources"] == 0
    assert counts["reference_coverage_verified"] is False
    assert counts["population_coverage_complete"] is False


def test_detached_minutes_cannot_verify_first_discovery(tmp_path):
    from tape_data_product.acquisition.vendor import acquire_tq

    lo, _ = bounds(DAY)
    reference = jsonl(
        tmp_path / "reference.jsonl",
        [dict(session_date=DAY, symbol="A", type="CS")],
    )
    # A cropped file can conceal an earlier trigger. Its observed trigger is
    # measurable, but must not be represented as verified first discovery.
    minutes = jsonl(
        tmp_path / "minutes.jsonl",
        [bar("A", lo + 10 * MINUTE_NS), bar("A", lo + 11 * MINUTE_NS)],
    )
    selection = screen(reference, minutes, DAY, tmp_path / "screen")
    selected = json.loads(selection.read_text())
    assert selected["discovery_verified"] is False
    assert selected["verified"] is False
    assert selected["minute_source_stage_identity"] is None
    with pytest.raises(ValueError, match="verified first-discovery"):
        acquire_tq(FixtureClient([]), selection, tmp_path / "tq")


def test_acquired_pair_binds_exact_verified_selection_record(tmp_path):
    import hashlib
    from tape_data_product.acquisition.vendor import acquire_tq
    from tape_data_product.stages import verify_stage

    lo, _ = bounds(DAY)
    reference = acquire_reference(
        FixtureClient([{"status": "OK", "results": [{"ticker": "A", "type": "CS"}]}]),
        DAY,
        tmp_path / "reference",
    )
    minute_rows = [
        {"t": ts // 1_000_000, "n": 800, "v": 100, "o": 10, "c": 10, "h": 11, "l": 10}
        for ts in (lo, lo + MINUTE_NS)
    ]
    minutes = acquire_minutes(
        FixtureClient([{"status": "OK", "results": minute_rows}]),
        DAY,
        "A",
        tmp_path / "minutes",
    )
    selection = screen(
        reference, minutes, DAY, tmp_path / "screen", minute_receipts=[minutes.parent]
    )
    selected = json.loads(selection.read_text())
    assert selected["discovery_verified"] is True
    assert selected["minute_source_stage_identity"] == verify_stage(minutes.parent)["identity"]
    acquire_tq(
        FixtureClient([{"status": "OK", "results": []}, {"status": "OK", "results": []}]),
        selection,
        tmp_path / "acquired",
    )
    pair = json.loads(
        (tmp_path / "acquired" / f"tq/session_date={DAY}/symbol=A/pair.json").read_text()
    )
    assert pair["selection_record"] == selected
    expected_digest = hashlib.sha256(
        json.dumps(selected, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()
    assert pair["selection_record_sha256"] == expected_digest
    assert pair["selection_stage_identity"] == verify_stage(selection.parent)["identity"]
