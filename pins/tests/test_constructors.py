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
    rel_path = p_file.relative_to(p_cache)

    # parents has every entry you'd get if you called .parents all the way to some root.
    # for a relative path, the root is likely ".", so we subtract 1 to get the number
    # of parent directories.
    # note this essentially counts slashes, in a inter-OS friendly way.
    n_parents = len(rel_path.parents) - 1
    assert n_parents == 2


def construct_from_board(board):
    prot = board.fs.protocol
    fs_name = prot if isinstance(prot, str) else prot[0]

    if fs_name in ["file", ("file", "local")]:
        board = c.board_folder(board.board)
    elif fs_name == "rsc":
        board = c.board_rsconnect(
            server_url=board.fs.api.server_url, api_key=board.fs.api.api_key
        )
    elif fs_name == "abfs":
        board = c.board_azure(board.board)
    elif fs_name == "gs":
        board = c.board_gcs(board.board)
    else:
        board = getattr(c, f"board_{fs_name}")(board.board)

    return board


# End-to-end constructor tests

# there are two facets of boards: reading and writing.
# copied from test_compat
@pytest.mark.skip_on_github
def test_constructor_board_url_data(tmp_cache, http_example_board_path, df_csv):
    board = c.board_url(
        http_example_board_path,
        # could derive from example version path
        pin_paths={"df_csv": "df_csv/20220214T163720Z-9bfad/"},
    )

    df = board.pin_read("df_csv")

    # check data ----
    assert_frame_equal(df, df_csv)


@pytest.mark.xfail
@pytest.mark.skip_on_github
def test_constructor_board_url_cache(
    tmp_cache, http_example_board_path, df_csv, tmp_path
):
    # TODO: downloading a pin does not put files in the same directory, since
    # in this case we are hashing on the full url.

    board = c.board_url(
        http_example_board_path,
        # could derive from example version path
        pin_paths={"df_csv": "df_csv/20220214T163718Z-eceac/"},
    )

    board.pin_read("df_csv")

    # cannot write or view pin versions

    with pytest.raises(NotImplementedError):
        board.pin_write(df_csv)
    with pytest.raises(NotImplementedError):
        board.pin_versions("df_csv")
    with pytest.raises(NotImplementedError):
        board.pin_version_delete(name="df_csv", version="20220214T163718Z")
    with pytest.raises(NotImplementedError):
        df = pd.DataFrame({"x": [1, 2, 3]})
        path = tmp_path / "data.csv"
        df.to_csv(path, index=False)
        board.pin_upload(path, "cool_pin")

    # check cache ----
    http_dirs = list(tmp_cache.glob("http_*"))

    assert len(http_dirs) == 1

    # there are two files in the flat cache (metadata, and the csv)
    parent = http_dirs[0]
    res = list(parent.rglob("*"))
    assert len(res) == 2

    # validate that it creates an empty metadata file
    assert len(x for x in res if x.endswith("df_csv.csv")) == 1
    assert len(x for x in res if x.endswith("data.txt")) == 1

    assert len(list(parent.glob("**/*"))) == 2


@pytest.mark.skip_on_github
def test_constructor_board_url_file(tmp_cache, http_example_board_path):
    # TODO: downloading a pin does not put files in the same directory, since
    # in this case we are hashing on the full url.

    board = c.board_url(
        http_example_board_path,
        # could derive from example version path
        pin_paths={"df_csv": "df_csv/20220214T163718Z-eceac/df_csv.csv"},
    )

    board.pin_download("df_csv")

    # check cache ----
    http_dirs = list(tmp_cache.glob("http_*"))

    assert len(http_dirs) == 1

    # there are two files in the flat cache (metadata, and the csv)
    parent = http_dirs[0]
    res = list(parent.rglob("*"))
    assert len(res) == 1

    assert str(res[0]).endswith("df_csv.csv")

    new_board = eval(c.board_deparse(board), c.__dict__)
    assert new_board.pin_list() == board.pin_list()


@pytest.mark.skip_on_github
def test_constructor_board_github(tmp_cache, http_example_board_path, df_csv):
    board = c.board_github("rstudio", "pins-python", EXAMPLE_REL_PATH)  # noqa

    df = board.pin_read("df_csv")
    assert_frame_equal(df, df_csv)

    cache_options = list(tmp_cache.glob("github_*"))
    assert len(cache_options) == 1
    cache_dir = cache_options[0]

    res = list(cache_dir.rglob("**/*.csv"))
    assert len(res) == 1

    check_cache_file_path(res[0], cache_dir)


@pytest.fixture(scope="function")
def board(backend):
    # TODO: copied from test_compat.py

    board = backend.create_tmp_board(str(PATH_TO_EXAMPLE_BOARD.absolute()))
    yield board
    backend.teardown_board(board)


def test_constructor_boards(board, df_csv, tmp_cache):
    # TODO: would be nice to have fixtures for each board constructor
    # doesn't need to copy over pins-compat content

    # create board from constructor -------------------------------------------
    board = construct_from_board(board)

    # read a pin and check its contents ---------------------------------------

    df = board.pin_read("df_csv")

    # check data
    assert_frame_equal(df, df_csv)

    # check the cache structure -----------------------------------------------

    # check cache
    if board.fs.protocol in ["file", ("file", "local")]:
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


@pytest.fixture(scope="function")
def board2(backend):
    board2 = backend.create_tmp_board()
    yield board2
    backend.teardown_board(board2)


def test_constructor_boards_multi_user(board2, df_csv, tmp_cache):
    prot = board2.fs.protocol
    fs_name = prot if isinstance(prot, str) else prot[0]

    if fs_name == "rsc":
        # TODO: RSConnect writes pin names like <user>/<name>, so would need to
        # modify test
        pytest.skip()
    elif fs_name == "abfs":
        fs_name = "azure"

    first = construct_from_board(board2)

    first.pin_write(df_csv, "df_csv", type="csv")
    assert first.pin_list() == ["df_csv"]

    second = construct_from_board(board2)
    second.pin_write(df_csv, "another_df_csv", type="csv")

    assert sorted(second.pin_list()) == sorted(["df_csv", "another_df_csv"])


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


# Deparsing ===================================================================


def test_board_deparse(board):
    prot = board.fs.protocol

    with rm_env("CONNECT_API_KEY"):
        if prot == "rsc":
            os.environ["CONNECT_API_KEY"] = board.fs.api.api_key

        new_board = eval(c.board_deparse(board), c.__dict__)
        new_board.pin_list()
