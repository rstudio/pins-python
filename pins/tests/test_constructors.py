import os
import pytest

from pathlib import Path

from pins import constructors as c
from pins.tests.conftest import PATH_TO_EXAMPLE_BOARD
from pins.tests.helpers import rm_env


# adapted from https://stackoverflow.com/a/34333710


def check_dir_writable(p_dir):
    assert p_dir.parent.exists()
    assert os.access(p_dir.parent.absolute(), os.W_OK)


# End-to-end constructor tests

# there are two facets of boards: reading and writing.
# copied from test_compat
def test_constructor_board_url(tmp_cache, http_example_board_path):
    board = c.board_urls(
        http_example_board_path, pin_paths={"df_csv": "df_csv/20220214T163718Z-eceac/"}
    )

    board.pin_read("df_csv")

    # check cache
    # check data


def test_constructor_board_github(tmp_cache, http_example_board_path):
    board = c.board_github("machow", "pins-python", PATH_TO_EXAMPLE_BOARD)  # noqa


@pytest.fixture(scope="session")
def board(backend):
    # TODO: copied from test_compat.py

    board = backend.create_tmp_board(str(PATH_TO_EXAMPLE_BOARD.absolute()))
    yield board
    backend.teardown_board(board)


def test_constructor_board(board):
    prot = board.fs.protocol

    fs_name = prot if isinstance(prot, str) else prot[0]

    if fs_name == "file":
        con_name = "local"
    elif fs_name == "rsc":
        con_name = "rsconnect"
        pytest.xfail()
    else:
        con_name = fs_name

    board = getattr(c, f"board_{con_name}")(board.board)

    # check cache
    # check data


# Board particulars ===========================================================


@pytest.mark.skip_on_github
def test_board_constructor_local_default_writable():

    with rm_env("PINS_DATA_DIR"):
        board = c.board_local()
        p_board = Path(board.board)

        check_dir_writable(p_board)
        assert p_board.name == "pins-py"


def test_board_constructor_temp_writable():
    with rm_env("PINS_DATA_DIR"):
        board = c.board_temp()
        p_board = Path(board.board)

        check_dir_writable(p_board)
        assert len(list(p_board.glob("*"))) == 0


def test_board_constructor_folder(tmp_dir2, df):
    board = c.board_folder(str(tmp_dir2))
    board.pin_write(df, "some_df", type="csv")

    assert (tmp_dir2 / "some_df").exists()
    df2 = board.pin_read("some_df")

    assert df.equals(df2)
