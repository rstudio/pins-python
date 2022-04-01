import pytest
import tempfile
import os

from importlib_resources import files
from pytest import mark as m
from pathlib import Path
from pins.tests.helpers import BoardBuilder, RscBoardBuilder, Snapshot, rm_env

EXAMPLE_REL_PATH = "pins/tests/pins-compat"
PATH_TO_EXAMPLE_BOARD = files("pins") / "tests/pins-compat"
PATH_TO_EXAMPLE_VERSION = PATH_TO_EXAMPLE_BOARD / "df_csv/20220214T163720Z-9bfad/"
EXAMPLE_PIN_NAME = "df_csv"


# Based on https://github.com/machow/siuba/blob/main/siuba/tests/helpers.py
BACKEND_MARKS = ["fs_s3", "fs_file", "fs_rsc"]

# parameters that can be used more than once per session
params_safe = [
    pytest.param(lambda: BoardBuilder("file"), id="file", marks=m.fs_file),
    pytest.param(lambda: BoardBuilder("s3"), id="s3", marks=m.fs_s3),
]

# rsc should only be used once, because users are created at docker setup time
param_rsc = pytest.param(lambda: RscBoardBuilder("rsc"), id="rsc", marks=m.fs_rsc)

params_backend = [*params_safe, param_rsc]


@pytest.fixture(params=params_backend, scope="session")
def backend(request):
    backend = request.param()
    yield backend
    backend.teardown()


@pytest.fixture(scope="session")
def http_example_board_path():
    # backend = BoardBuilder("s3")
    # yield backend.create_tmp_board(str(PATH_TO_EXAMPLE_BOARD.absolute())).board
    # backend.teardown()
    # TODO: could putting it in a publically available bucket folder
    return "https://raw.githubusercontent.com/machow/pins-python/main/pins/tests/pins-compat"


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


@pytest.fixture
def tmp_cache():
    with rm_env("PINS_CACHE_DIR"):
        with tempfile.TemporaryDirectory() as tmp_dir:
            os.environ["PINS_CACHE_DIR"] = str(tmp_dir)
            yield Path(tmp_dir)


@pytest.fixture
def tmp_data_dir():
    with rm_env("PINS_DATA_DIR"):
        with tempfile.TemporaryDirectory() as tmp_dir:
            os.environ["PINS_DATA_DIR"] = str(tmp_dir)
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
