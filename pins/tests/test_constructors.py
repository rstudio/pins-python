import os
import pandas as pd
import pytest

from pandas.testing import assert_frame_equal
from pathlib import Path

from pins import constructors as c
from pins.tests.conftest import (
    PATH_TO_EXAMPLE_BOARD,
    PATH_TO_EXAMPLE_VERSION,
    EXAMPLE_REL_PATH,
)
from pins.tests.helpers import rm_env


@pytest.fixture
def df_csv():
    return pd.read_csv(PATH_TO_EXAMPLE_VERSION / "df_csv.csv")


def check_dir_writable(p_dir):
    assert p_dir.parent.exists()
    assert os.access(p_dir.parent.absolute(), os.W_OK)


def check_cache_file_path(p_file, p_cache):
    assert str(p_file.relative_to(p_cache)).count("/") == 2


# End-to-end constructor tests

# there are two facets of boards: reading and writing.
# copied from test_compat
@pytest.mark.skip_on_github
def test_constructor_board_url_data(tmp_cache, http_example_board_path, df_csv):
    board = c.board_urls(
        http_example_board_path,
        # could derive from example version path
        pin_paths={"df_csv": "df_csv/20220214T163720Z-9bfad/"},
    )

    df = board.pin_read("df_csv")

    # check data ----
    assert_frame_equal(df, df_csv)


@pytest.mark.xfail
@pytest.mark.skip_on_github
def test_constructor_board_url_cache(tmp_cache, http_example_board_path, df_csv):
    # TODO: downloading a pin does not put files in the same directory, since
    # in this case we are hashing on the full url.

    board = c.board_urls(
        http_example_board_path,
        # could derive from example version path
        pin_paths={"df_csv": "df_csv/20220214T163718Z-eceac/"},
    )

    board.pin_read("df_csv")

    # check cache ----
    http_dirs = list(tmp_cache.glob("http_*"))

    assert len(http_dirs) == 1

    parent = http_dirs[0]
    res = list(parent.rglob("**/*.csv"))
    assert len(res) == 1

    # has form: <pin>/<version>/<file>
    check_cache_file_path(res[0], parent)


@pytest.mark.skip_on_github
def test_constructor_board_github(tmp_cache, http_example_board_path, df_csv):
    board = c.board_github("machow", "pins-python", EXAMPLE_REL_PATH)  # noqa

    df = board.pin_read("df_csv")
    assert_frame_equal(df, df_csv)

    cache_options = list(tmp_cache.glob("github_*"))
    assert len(cache_options) == 1
    cache_dir = cache_options[0]

    res = list(cache_dir.rglob("**/*.csv"))
    assert len(res) == 1

    check_cache_file_path(res[0], cache_dir)


@pytest.fixture(scope="session")
def board(backend):
    # TODO: copied from test_compat.py

    board = backend.create_tmp_board(str(PATH_TO_EXAMPLE_BOARD.absolute()))
    yield board
    backend.teardown_board(board)


def test_constructor_board(board, df_csv, tmp_cache):
    # TODO: would be nice to have fixtures for each board constructor
    # doesn't need to copy over pins-compat content

    # create board from constructor -------------------------------------------

    prot = board.fs.protocol
    fs_name = prot if isinstance(prot, str) else prot[0]

    if fs_name == "file":
        board = c.board_folder(board.board)
    elif fs_name == "rsc":
        board = c.board_rsconnect(
            server_url=board.fs.api.server_url, api_key=board.fs.api.api_key
        )
    else:
        board = getattr(c, f"board_{fs_name}")(board.board)

    # read a pin and check its contents ---------------------------------------

    df = board.pin_read("df_csv")

    # check data
    assert_frame_equal(df, df_csv)

    # check the cache structure -----------------------------------------------

    # check cache
    if fs_name == "file":
        # no caching for local file boards
        pass
    else:
        # check path structure ----

        options = list(tmp_cache.glob("*"))
        assert len(options) == 1

        cache_dir = options[0]
        res = list(cache_dir.rglob("*/*.csv"))
        assert len(res) == 1

        check_cache_file_path(res[0], cache_dir)

        # check cache touch on access time ----

        meta = board.pin_meta("df_csv")
        p_cache_meta = (
            Path(board._get_cache_path(meta.name, meta.version.version)) / "data.txt"
        )
        orig_access = p_cache_meta.stat().st_atime

        board.pin_meta("df_csv")

        new_access = p_cache_meta.stat().st_atime

        assert orig_access < new_access


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
