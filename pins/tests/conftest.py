import pytest
import tempfile

from importlib_resources import files
from pytest import mark as m
from pathlib import Path
from pins.tests.helpers import BoardBuilder, RscBoardBuilder, Snapshot

# Based on https://github.com/machow/siuba/blob/main/siuba/tests/helpers.py
BACKEND_MARKS = ["fs_s3", "fs_file", "fs_rsc"]

param_rsc = pytest.param(lambda: RscBoardBuilder("rsc"), id="rsc", marks=m.fs_rsc)

params_backend = [
    pytest.param(lambda: BoardBuilder("file"), id="file", marks=m.fs_file),
    pytest.param(lambda: BoardBuilder("s3"), id="s3", marks=m.fs_s3),
    param_rsc,
]


@pytest.fixture(params=params_backend, scope="session")
def backend(request):
    backend = request.param()
    yield backend
    backend.teardown()


@pytest.fixture
def snapshot(request):
    p_snap = files("pins") / "tests/_snapshots" / request.node.originalname
    snap = Snapshot(p_snap, request.config.getoption("--snapshot-update"))

    return snap


@pytest.fixture
def df():
    import pandas as pd

    return pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})


@pytest.fixture
def tmp_dir2():
    # fixture for offering a temporary directory
    # note that pytest has a built-in fixture tmp_dir, but it uses the lib py.path
    # which recommends using pathlib, etc..
    with tempfile.TemporaryDirectory() as tmp_dir:
        yield Path(tmp_dir)


def pytest_addoption(parser):
    parser.addoption("--snapshot-update", action="store_true")


def pytest_configure(config):
    # TODO: better way to define all marks? Can we iterate over params above?
    for mark_name in BACKEND_MARKS:
        fs_name = mark_name.split("_")[-1]
        config.addinivalue_line(
            "markers", f"{mark_name}: mark test to only run on {fs_name} filesystem."
        )
