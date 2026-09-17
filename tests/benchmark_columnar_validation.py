"""Deterministic validator-only microbenchmark; fixture construction is untimed."""
import argparse
import gc
import json
import statistics
import time

import pyarrow as pa

from scalar_validation_reference import validate_batch as scalar_validate_batch
from tape_data_product.contracts import BASE_SCHEMA, FEATURE_SCHEMA, SUPPORT_SCHEMA, feature_registry
from tape_data_product.contracts.validation import validate_batch
from test_endpoint_contracts import base_row, feature_row


ROWS = 4096


def fixtures():
    base_rows = []
    feature_rows = []
    support_rows = []
    for index in range(ROWS):
        second = index + 1
        base = base_row(second)
        if index % 11 == 0:
            base.update(
                bid_size_end_shares=None,
                ask_size_end_shares=None,
                bid_size_end_reason_mask=64,
                ask_size_end_reason_mask=64,
            )
        base_rows.append(base)

        feature = feature_row()
        feature["interval_end_ns"] = base["interval_end_ns"]
        if index % 13 == 0:
            for item in feature_registry():
                feature[item.name] = None
                feature[item.name + "_reason_mask"] = 1
        feature_rows.append(feature)

        support = {field.name: 0 for field in SUPPORT_SCHEMA}
        support.update(session_date=base["session_date"], symbol=base["symbol"],
                       interval_end_ns=base["interval_end_ns"])
        for name in SUPPORT_SCHEMA.names:
            if "_possible_" in name:
                support[name] = 100.0
            elif "_usable_" in name:
                support[name] = 80.0
        support_rows.append(support)
    return {
        "base": pa.RecordBatch.from_pylist(base_rows, schema=BASE_SCHEMA),
        "features": pa.RecordBatch.from_pylist(feature_rows, schema=FEATURE_SCHEMA),
        "support": pa.RecordBatch.from_pylist(support_rows, schema=SUPPORT_SCHEMA),
    }


def measure(validator, batches, repeats):
    for kind, batch in batches.items():
        for _ in range(3):
            validator(batch, kind)
    result = {}
    for kind, batch in batches.items():
        samples = []
        for _ in range(repeats):
            gc.collect()
            start = time.perf_counter()
            validator(batch, kind)
            samples.append(time.perf_counter() - start)
        median = statistics.median(samples)
        result[kind] = {"median_seconds": median, "rows_per_second": ROWS / median}
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backend", choices=("scalar", "columnar", "both"), default="both")
    parser.add_argument("--repeats", type=int, default=9)
    args = parser.parse_args()
    batches = fixtures()
    validators = {
        "scalar": scalar_validate_batch,
        "columnar": validate_batch,
    }
    selected = validators if args.backend == "both" else {args.backend: validators[args.backend]}
    output = {name: measure(validator, batches, args.repeats)
              for name, validator in selected.items()}
    if args.backend == "both":
        output["speedup"] = {
            kind: output["scalar"][kind]["median_seconds"] /
                  output["columnar"][kind]["median_seconds"]
            for kind in batches
        }
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
