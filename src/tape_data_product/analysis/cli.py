"""Command-line dispatch for the retained endpoint/EW V2 report analyses."""

from pathlib import Path


def _population(args):
    from .endpoint_population import render_daily_completed_members

    return render_daily_completed_members(
        args.inventory,
        args.output,
        expected_members=args.expected_members,
        session_hours=args.session_hours,
        dpi=args.dpi,
    )


def _activity_population(args):
    from .endpoint_population import render_daily_active_tape_hours

    return render_daily_active_tape_hours(
        args.member_gate_accounting,
        args.config,
        args.output,
        half_life_seconds=args.half_life_seconds,
        session=args.session,
        expected_members=args.expected_members,
        expected_represented_seconds=args.expected_represented_seconds,
        dpi=args.dpi,
    )


def _activity_ecdf_calculate(args):
    from .activity_definition_ecdf import calculate

    result = calculate(
        args.inventory,
        args.output,
        expected_members=args.expected_members,
        probability_steps=args.probability_steps,
        memory_limit=args.memory_limit,
        maximum_spill=args.maximum_spill,
        resume=args.resume,
        method=args.method,
        log_bin_width=args.log_bin_width,
    )
    return {
        "schema": result["schema"],
        "members": result["members"],
        "represented_rows": result["represented_rows"],
        "output": str(args.output / "numerical.json"),
    }


def _activity_ecdf_render(args):
    from .activity_definition_ecdf import render

    return render(args.numerical, args.output, dpi=args.dpi)


def _feature_ecdf_calculate(args):
    from .main_feature_ecdf import calculate

    result = calculate(
        args.inventory,
        args.output,
        families=tuple(args.families),
        maximum_members=args.maximum_members,
        batch_size=args.batch_size,
        resume=args.resume,
    )
    return {
        "schema": result["schema"],
        "members": result["members"],
        "families": list(result["families"]),
        "output": str(args.output / "numerical.json"),
    }


def _feature_ecdf_render(args):
    from .main_feature_ecdf import render

    return render(args.numerical, args.output, dpi=args.dpi)


def _joint(args):
    from .endpoint_joint_expanded import run

    return run(args)


def _joint_render(args):
    from .endpoint_joint_expanded import render_saved

    return render_saved(args.input, population_label=args.population_label)


def _gpus_cast(args):
    from .gpus_cast_comparison import run

    panels = run(
        args.numerical_source,
        [args.gpus_base_partition, args.cast_base_partition],
        args.png,
        args.svg,
    )
    return {
        "state": "complete",
        "endpoints": [
            f"{panel['member']} {panel['endpoint']} ET" for panel in panels
        ],
        "png": str(args.png),
        "svg": str(args.svg),
    }


def register_commands(subparsers):
    report = subparsers.add_parser(
        "report", help="Calculate and render endpoint/EW V2 report artifacts"
    )
    commands = report.add_subparsers(dest="report_command", required=True)

    population = commands.add_parser(
        "population", help="Render daily completed-member population artifacts"
    )
    population.add_argument("--inventory", type=Path, required=True)
    population.add_argument("--output", type=Path, required=True)
    population.add_argument("--expected-members", type=int)
    population.add_argument("--session-hours", type=float, default=16.0)
    population.add_argument("--dpi", type=int, default=190)
    population.set_defaults(func=_population)

    activity_population = commands.add_parser(
        "activity-population", help="Render active-tape stock-hours by date"
    )
    activity_population.add_argument(
        "--member-gate-accounting", type=Path, required=True
    )
    activity_population.add_argument("--config", type=Path, required=True)
    activity_population.add_argument("--output", type=Path, required=True)
    activity_population.add_argument("--half-life-seconds", type=int, default=30)
    activity_population.add_argument("--session", default="pooled")
    activity_population.add_argument("--expected-members", type=int)
    activity_population.add_argument("--expected-represented-seconds", type=int)
    activity_population.add_argument("--dpi", type=int, default=190)
    activity_population.set_defaults(func=_activity_population)

    activity_calculate = commands.add_parser(
        "activity-ecdf-calculate",
        help="Calculate ungated ECDFs for the active-tape gate inputs",
    )
    activity_calculate.add_argument("--inventory", type=Path, required=True)
    activity_calculate.add_argument("--output", type=Path, required=True)
    activity_calculate.add_argument("--expected-members", type=int)
    activity_calculate.add_argument("--probability-steps", type=int, default=10_000)
    activity_calculate.add_argument("--memory-limit", default="1GiB")
    activity_calculate.add_argument("--maximum-spill", default="24GiB")
    activity_calculate.add_argument("--resume", action="store_true")
    activity_calculate.add_argument(
        "--method", choices=("exact_rank", "log_binned"), default="exact_rank"
    )
    activity_calculate.add_argument("--log-bin-width", type=float, default=0.0025)
    activity_calculate.set_defaults(func=_activity_ecdf_calculate)

    activity_render = commands.add_parser(
        "activity-ecdf-render", help="Render saved active-tape gate-input ECDFs"
    )
    activity_render.add_argument("--numerical", type=Path, required=True)
    activity_render.add_argument("--output", type=Path, required=True)
    activity_render.add_argument("--dpi", type=int, default=190)
    activity_render.set_defaults(func=_activity_ecdf_render)

    from .main_feature_ecdf import FAMILIES

    feature_calculate = commands.add_parser(
        "feature-ecdf-calculate", help="Calculate active-tape V2 feature ECDFs"
    )
    feature_calculate.add_argument("--inventory", type=Path, required=True)
    feature_calculate.add_argument("--output", type=Path, required=True)
    feature_calculate.add_argument(
        "--families", nargs="+", choices=tuple(FAMILIES), default=list(FAMILIES)
    )
    feature_calculate.add_argument("--maximum-members", type=int)
    feature_calculate.add_argument("--batch-size", type=int, default=25_000)
    feature_calculate.add_argument("--resume", action="store_true")
    feature_calculate.set_defaults(func=_feature_ecdf_calculate)

    feature_render = commands.add_parser(
        "feature-ecdf-render", help="Render saved active-tape feature ECDFs"
    )
    feature_render.add_argument("--numerical", type=Path, required=True)
    feature_render.add_argument("--output", type=Path, required=True)
    feature_render.add_argument("--dpi", type=int, default=190)
    feature_render.set_defaults(func=_feature_ecdf_render)

    joint = commands.add_parser(
        "joint", help="Calculate full-reference V2 joint distributions"
    )
    joint.add_argument("--reference", required=True)
    joint.add_argument("--reference-identity", required=True)
    joint.add_argument("--base-root", required=True)
    joint.add_argument("--features-root", required=True)
    joint.add_argument("--output", required=True)
    joint.add_argument("--expected-members", type=int)
    joint.add_argument(
        "--cache-state", default="uncontrolled shared OS cache; not labeled cold"
    )
    joint.add_argument("--rss-stop-bytes", type=int, default=2 * 1024**3)
    joint.set_defaults(func=_joint)

    joint_render = commands.add_parser(
        "joint-render", help="Re-render saved V2 joint-distribution tables"
    )
    joint_render.add_argument("--input", type=Path, required=True)
    joint_render.add_argument("--population-label")
    joint_render.set_defaults(func=_joint_render)

    comparison = commands.add_parser(
        "gpus-cast", help="Render the fixed GPUS/CAST V2 worked example"
    )
    comparison.add_argument("--numerical-source", type=Path, required=True)
    comparison.add_argument("--gpus-base-partition", type=Path, required=True)
    comparison.add_argument("--cast-base-partition", type=Path, required=True)
    comparison.add_argument("--png", type=Path, required=True)
    comparison.add_argument("--svg", type=Path, required=True)
    comparison.set_defaults(func=_gpus_cast)
