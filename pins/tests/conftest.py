import os
import tempfile
from pathlib import Path

import pytest
from importlib_resources import files
from pytest import mark as m

from pins.tests.helpers import BoardBuilder, RscBoardBuilder, DbcBoardBuilder, Snapshot, rm_env

EXAMPLE_REL_PATH = "pins/tests/pins-compat"
PATH_TO_EXAMPLE_BOARD = files("pins") / "tests/pins-compat"
PATH_TO_EXAMPLE_VERSION = PATH_TO_EXAMPLE_BOARD / "df_csv/20220214T163720Z-9bfad/"
EXAMPLE_PIN_NAME = "df_csv"

PATH_TO_MANIFEST_BOARD = files("pins") / "tests/pin-board"

# parameters that can be used more than once per session
params_safe = [
    pytest.param(lambda: BoardBuilder("file"), id="file", marks=m.fs_file),
    pytest.param(lambda: BoardBuilder("s3"), id="s3", marks=m.fs_s3),
    pytest.param(lambda: BoardBuilder("gcs"), id="gcs", marks=m.fs_gcs),
    pytest.param(lambda: BoardBuilder("abfs"), id="abfs", marks=m.fs_abfs),
]

# rsc should only be used once, because users are created at docker setup time
param_rsc = pytest.param(lambda: RscBoardBuilder("rsc"), id="rsc", marks=m.fs_rsc)
param_dbc = pytest.param(lambda: DbcBoardBuilder("dbc"), id="dbc", marks=m.fs_dbc)

params_backend = [*params_safe, param_rsc, param_dbc]


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
    return (
        "https://raw.githubusercontent.com/machow/pins-python/main/pins/tests/pins-compat"
    )


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
