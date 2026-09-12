"""Mathematical verification for report-plot artifacts and SVG exports.

The renderer writes an expected-geometry sidecar from numerical ECDF vertices.
This module independently reads the exported SVG paths, accounts for SVG decimal
rounding, and proves that no backend simplification changed the step geometry.
It deliberately verifies SVG (the vector deliverable); PNG is identity-bound to
the same render configuration in the bundle manifest.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
import xml.etree.ElementTree as ET

SCHEMA = "report_plot_geometry_v1"
_NUMBER = re.compile(r"[-+]?(?:\d*\.\d+|\d+)(?:[eE][-+]?\d+)?")


class VerificationError(ValueError):
    """Raised when a rendered export is not a faithful numerical display."""


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def canonical_identity(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode()).hexdigest()


def _svg_path(svg: Path, artist_id: str) -> list[tuple[float, float]]:
    root = ET.parse(svg).getroot()
    group = next(
        (node for node in root.iter() if node.attrib.get("id") == artist_id), None
    )
    if group is None:
        raise VerificationError(f"missing SVG artist {artist_id}")
    paths = [
        node
        for node in group.iter()
        if node.tag.endswith("path") and node.attrib.get("d")
    ]
    if len(paths) != 1:
        raise VerificationError(
            f"expected one SVG path for {artist_id}, found {len(paths)}"
        )
    commands = re.findall(
        r"[A-Za-z]|[-+]?(?:\d*\.\d+|\d+)(?:[eE][-+]?\d+)?", paths[0].attrib["d"]
    )
    if any(token.isalpha() and token not in ("M", "L") for token in commands):
        raise VerificationError(f"nonlinear SVG command in ECDF artist {artist_id}")
    numbers = [float(token) for token in commands if not token.isalpha()]
    if len(numbers) % 2:
        raise VerificationError(f"odd SVG coordinate count for {artist_id}")
    return list(zip(numbers[::2], numbers[1::2]))


def _point_error(
    actual: list[tuple[float, float]], expected: list[list[float]]
) -> float:
    if len(actual) != len(expected):
        raise VerificationError(
            f"vertex count changed: SVG={len(actual)}, expected={len(expected)}"
        )
    return max(
        (
            max(abs(ax - ex), abs(ay - ey))
            for (ax, ay), (ex, ey) in zip(actual, expected)
        ),
        default=0.0,
    )


def verify_svg(svg: Path, geometry: Path) -> dict:
    """Verify every named ECDF path and its advertised final error bound."""
    reference = json.loads(geometry.read_text())
    if reference.get("schema") != SCHEMA:
        raise VerificationError("unsupported geometry-reference schema")
    rounding_decimals = int(reference["svg_coordinate_decimals"])
    rounding_tolerance_pt = 0.5 * 10.0 ** (-rounding_decimals) + 1e-9
    curves = []
    for curve in reference["curves"]:
        actual = _svg_path(svg, curve["artist_id"])
        error_pt = _point_error(actual, curve["svg_points"])
        if error_pt > rounding_tolerance_pt:
            raise VerificationError(
                f"{curve['artist_id']} geometry error {error_pt:.9g} pt exceeds "
                f"rounding allowance {rounding_tolerance_pt:.9g} pt"
            )
        axes_height_pt = float(curve["axes_height_pt"])
        if not math.isfinite(axes_height_pt) or axes_height_pt <= 0:
            raise VerificationError("invalid axes height")
        rounding_error_pp = (
            error_pt * float(curve["y_span_percentage_points"]) / axes_height_pt
        )
        final_bound_pp = float(curve["reduction_error_bound_pp"]) + rounding_error_pp
        claimed = curve.get("claimed_final_error_bound_pp")
        if claimed is not None and final_bound_pp > float(claimed) + 1e-12:
            raise VerificationError(
                f"{curve['artist_id']} final bound {final_bound_pp:.9g} pp exceeds claim {claimed} pp"
            )
        curves.append(
            {
                "artist_id": curve["artist_id"],
                "vertices": len(actual),
                "max_coordinate_error_pt": error_pt,
                "coordinate_rounding_allowance_pt": rounding_tolerance_pt,
                "rounding_error_bound_pp": rounding_error_pp,
                "reduction_error_bound_pp": float(curve["reduction_error_bound_pp"]),
                "final_display_error_bound_pp": final_bound_pp,
                "claimed_final_error_bound_pp": claimed,
                "passed": True,
            }
        )
    return {
        "state": "passed",
        "svg": str(svg),
        "svg_sha256": sha256(svg),
        "geometry": str(geometry),
        "geometry_sha256": sha256(geometry),
        "coordinate_rounding": f"SVG coordinates rounded to {rounding_decimals} decimal places",
        "curves": curves,
    }


def verify_bundle(output: Path, expected_config_identity: str) -> dict:
    """Verify every page and reject mixed PNG/vector render configurations."""
    page_records = []
    for sidecar in sorted(output.glob("*.geometry.json")):
        stem = sidecar.name.removesuffix(".geometry.json")
        svg = output / f"{stem}.svg"
        png = output / f"{stem}.png"
        if not svg.is_file() or not png.is_file():
            raise VerificationError(f"missing PNG/SVG pair for {stem}")
        reference = json.loads(sidecar.read_text())
        if reference.get("render_config_identity") != expected_config_identity:
            raise VerificationError(f"configuration identity mismatch for {stem}")
        result = (
            verify_svg(svg, sidecar)
            if reference["curves"]
            else {
                "state": "passed",
                "svg": str(svg),
                "svg_sha256": sha256(svg),
                "geometry": str(sidecar),
                "geometry_sha256": sha256(sidecar),
                "curves": [],
            }
        )
        result.update(
            page=stem,
            png_sha256=sha256(png),
            render_config_identity=expected_config_identity,
        )
        page_records.append(result)
    if not page_records:
        raise VerificationError("no rendered pages found")
    return {
        "state": "passed",
        "render_config_identity": expected_config_identity,
        "pages": page_records,
    }


def reproduce_simplification_defect(output: Path) -> dict:
    """Isolate the historical erased-initial-atom defect on 202 coordinates."""
    import numpy as np
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    output.mkdir(parents=True, exist_ok=False)
    x = np.r_[0.0, 0.0, np.geomspace(0.00001, 50, 200)]
    y = np.r_[0.0, 2.0, np.linspace(2.01, 100, 200)]
    result = {}
    for simplify in (True, False):
        with matplotlib.rc_context({"path.simplify": simplify}):
            fig, ax = plt.subplots(figsize=(5, 3))
            (line,) = ax.step(x, y, where="post", gid="synthetic_ecdf")
            if not simplify:
                line.get_path().should_simplify = False
            ax.set_xscale("symlog", linthresh=0.1)
            ax.set(xlim=(0, 50), ylim=(0, 101))
            path = output / f"simplify_{str(simplify).lower()}.svg"
            fig.savefig(path)
            points = _svg_path(path, "synthetic_ecdf")
            same_x = [
                point[1] for point in points if abs(point[0] - points[0][0]) < 1e-6
            ]
            height = ax.get_window_extent().height * 72 / fig.dpi
            jump = (max(same_x) - min(same_x)) * 101 / height
            result[str(simplify).lower()] = {
                "svg": path.name,
                "svg_sha256": sha256(path),
                "vertices": len(points),
                "initial_vertical_jump_percentage_points": jump,
            }
            plt.close(fig)
    if not result["true"]["initial_vertical_jump_percentage_points"] < 0.05:
        raise VerificationError("simplification defect did not reproduce")
    if abs(result["false"]["initial_vertical_jump_percentage_points"] - 2) > 1e-5:
        raise VerificationError("unsimplified synthetic atom was not preserved")
    payload = {
        "state": "passed",
        "coordinates": 202,
        "specified_initial_atom_percentage_points": 2,
        "result": result,
        "conclusion": "backend simplification erases the atom; disabling it preserves the atom within SVG coordinate rounding",
    }
    (output / "results.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n"
    )
    return payload
