"""Render the app's route table and every route's response shape.

``python -m tests.contracts.render --write`` rewrites the golden files. Run it
only when an API change is intended and reviewed; the contract tests fail on
any difference from the committed goldens.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

import app as app_module
from tests.contracts import api_fixture
from tests.contracts.shapes import shape

HERE = Path(__file__).parent
ROUTES_FILE = HERE / "routes.json"
SHAPES_FILE = HERE / "api_shapes.json"


def route_table() -> list[list]:
    return [[route.path, sorted(route.methods)] for route in app_module.app.routes if isinstance(route, APIRoute)]


def response_shapes() -> dict[str, dict]:
    """Status code and payload shape of every route, served from the fixed fixture state."""
    monkeypatch = MonkeyPatch()
    try:
        api_fixture.install(monkeypatch)
        client = TestClient(app_module.app)
        shapes = {}
        for path, _methods in route_table():
            response = client.get(path.replace("{symbol}", api_fixture.SAMPLE_SYMBOL))
            shapes[path] = {"status_code": response.status_code, "shape": shape(response.json())}
        return shapes
    finally:
        monkeypatch.undo()


def dump(value) -> str:
    return json.dumps(value, indent=1, sort_keys=True) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--write", action="store_true", help="rewrite the golden files")
    args = parser.parse_args()
    routes, shapes = route_table(), response_shapes()
    if args.write:
        ROUTES_FILE.write_text(dump(routes))
        SHAPES_FILE.write_text(dump(shapes))
        print(f"wrote {ROUTES_FILE.name} ({len(routes)} routes) and {SHAPES_FILE.name}")
    else:
        print(dump({"routes": routes, "shapes": shapes}))


if __name__ == "__main__":
    main()
