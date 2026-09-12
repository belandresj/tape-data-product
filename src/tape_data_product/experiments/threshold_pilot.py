"""Fixed five-date descriptive participation study; no tuned outcomes or model."""

from pathlib import Path
import json
from tape_data_product.query.api import run_query
from tape_data_product.query.tape_cohort_config import normalize_config
from tape_data_product.stages import write_stage

DATES = ("2026-03-13", "2026-04-08", "2026-06-18", "2026-07-20", "2026-08-04")


def variants():
    base = json.loads(
        (
            Path(__file__).parents[1]
            / "resources/tape_cohort_300s_ms3_p050_selected_v1.json"
        ).read_text()
    )
    for name, bounds in [
        ("A", None),
        ("B", (0.4, 0.32)),
        ("C", (0.5, 0.4)),
        ("D", (0.6, 0.48)),
    ]:
        config = json.loads(json.dumps(base))
        if bounds is None:
            config["conditions"] = [
                row
                for row in config["conditions"]
                if row["feature"] != "movement_participation_300s"
            ]
        else:
            condition = next(
                row
                for row in config["conditions"]
                if row["feature"] == "movement_participation_300s"
            )
            condition["entry"]["lower"], condition["continuation"]["lower"] = bounds
        yield name, normalize_config(config)


def run_pilot(release, output, *, expected_release_hash=None):
    root = Path(output)
    root.mkdir(parents=True, exist_ok=False)
    results = []
    for name, config in variants():
        for day in DATES:
            directory = root / name / day
            summary = run_query(
                release,
                config,
                directory,
                expected_release_hash=expected_release_hash,
                dates=[day],
            )
            results.append(dict(variant=name, session_date=day, **summary))
    with (root / "aggregates.json").open("x") as stream:
        json.dump(results, stream, indent=2)
    write_stage(
        root,
        "participation_pilot",
        {"release_identity": results[0]["release_identity"]},
        {
            "dates": list(DATES),
            "variants": ["A", "B", "C", "D"],
            "study": "historical_descriptive_participation_pilot_v1",
        },
        [
            "aggregates.json",
            *[
                str(Path(row["variant"]) / row["session_date"] / "stage.json")
                for row in results
            ],
        ],
        {
            "completed_queries": 20,
            "interpretation": "dependent descriptive intervals; no predictive or executable expectancy",
        },
        synthetic=results[0]["synthetic"],
    )
    return {
        "completed_queries": 20,
        "output": str(root),
        "historical_comparison": "not_performed",
    }
