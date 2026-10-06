"""The existing public API is a contract: these tests fail on any change to it.

They pin the route table (paths, methods, order) and every route's status code
and response shape (keys and value types). An intended change is made by
regenerating the goldens with ``python -m tests.contracts.render --write`` and
reviewing the diff.
"""

import json

import pytest

from tests.contracts.render import ROUTES_FILE, SHAPES_FILE, response_shapes, route_table

GOLDEN_ROUTES = json.loads(ROUTES_FILE.read_text())
GOLDEN_SHAPES = json.loads(SHAPES_FILE.read_text())


@pytest.fixture(scope="module")
def shapes():
    return response_shapes()


def test_route_table_is_unchanged():
    assert route_table() == GOLDEN_ROUTES


def test_every_route_is_pinned():
    assert len(GOLDEN_ROUTES) == 92
    assert sorted(GOLDEN_SHAPES) == sorted(path for path, _ in GOLDEN_ROUTES)


@pytest.mark.parametrize("path", [path for path, _ in GOLDEN_ROUTES])
def test_response_contract_is_unchanged(shapes, path):
    assert shapes[path] == GOLDEN_SHAPES[path], (
        f"{path} changed its status code or response shape. If intended, run "
        "`python -m tests.contracts.render --write` and review the golden diff."
    )
