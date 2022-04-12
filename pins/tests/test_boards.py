import fsspec
import os
import uuid
import pandas as pd
import pytest

from pins.tests.helpers import DEFAULT_CREATION_DATE, rm_env
from pins.config import PINS_ENV_INSECURE_READ
from pins.errors import PinsError, PinsInsecureReadError
from pins.meta import MetaRaw

from datetime import datetime, timedelta
from time import sleep
from pathlib import Path

# using pytest cases, so that we can pass in fixtures as parameters
# TODO: this seems like maybe overkill
from pytest_cases import fixture, parametrize


@fixture
def df():
    import pandas as pd

    return pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})


@fixture
def board(backend):
    yield backend.create_tmp_board()
    backend.teardown()


# pin_write ===================================================================


def test_board_pin_write_default_title(board):

    df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
    meta = board.pin_write(df, "df_csv", title=None, type="csv")
    assert meta.title == "df_csv: a pinned 3 x 2 DataFrame"


def test_board_pin_write_prepare_pin(board, tmp_dir2):

    df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})

    meta = board.prepare_pin_version(
        str(tmp_dir2), df, "df_csv", title=None, type="csv"
    )
    assert meta.file == "df_csv.csv"
    assert (tmp_dir2 / "data.txt").exists()
    assert (tmp_dir2 / "df_csv.csv").exists()
    assert not (tmp_dir2 / "df_csv.csv").is_dir()


def test_board_pin_write_roundtrip(board):

    df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})

    assert not board.pin_exists("df_csv")

    board.pin_write(df, "df_csv", type="csv")

    assert board.pin_exists("df_csv")

    loaded_df = board.pin_read("df_csv")
    assert loaded_df.equals(df)


def test_board_pin_write_type_not_specified_error(board):
    class C:
        pass

    with pytest.raises(NotImplementedError):
        board.pin_write(C(), "cool_pin")


def test_board_pin_write_type_error(board):
    class C:
        pass

    with pytest.raises(NotImplementedError) as exc_info:
        board.pin_write(C(), "cool_pin", type="MY_TYPE")

    assert "MY_TYPE" in exc_info.value.args[0]


def test_board_pin_write_rsc_index_html(board, tmp_dir2, snapshot):
    if board.fs.protocol != "rsc":
        pytest.skip()

    df = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})

    pin_name = "test_rsc_pin"

    board.prepare_pin_version(
        str(tmp_dir2),
        df,
        pin_name,
        type="csv",
        title="some pin",
        description="some description",
        created=DEFAULT_CREATION_DATE,
    )

    snapshot.assert_equal_dir(tmp_dir2)


# pin_write against different types -------------------------------------------


@parametrize(
    "obj, type_", [(df, "csv"), (df, "joblib"), ({"a": 1, "b": [2, 3]}, "joblib")]
)
def test_board_pin_write_type(board, obj, type_, request):
    with rm_env(PINS_ENV_INSECURE_READ):
        os.environ[PINS_ENV_INSECURE_READ] = "1"
        meta = board.pin_write(obj, "test_pin", type=type_, title="some title")
        dst_obj = board.pin_read("test_pin")

        assert meta.type == type_

        if isinstance(obj, pd.DataFrame):
            assert obj.equals(dst_obj)

        obj == dst_obj


def test_board_pin_read_insecure_fail_default(board):
    board.pin_write({"a": 1}, "test_pin", type="joblib", title="some title")
    with pytest.raises(PinsInsecureReadError) as exc_info:
        board.pin_read("test_pin")

    assert "joblib" in exc_info.value.args[0]


def test_board_pin_read_insecure_fail_board_flag(board):
    # board flag prioritized over env var
    with rm_env(PINS_ENV_INSECURE_READ):
        os.environ[PINS_ENV_INSECURE_READ] = "1"
        board.allow_pickle_read = False
        board.pin_write({"a": 1}, "test_pin", type="joblib", title="some title")
        with pytest.raises(PinsInsecureReadError):
            board.pin_read("test_pin")


def test_board_pin_read_insecure_succeed_board_flag(board):
    # board flag prioritized over env var
    with rm_env(PINS_ENV_INSECURE_READ):
        os.environ[PINS_ENV_INSECURE_READ] = "0"
        board.allow_pickle_read = True
        board.pin_write({"a": 1}, "test_pin", type="joblib", title="some title")
        board.pin_read("test_pin")


# pin_delete ==================================================================


@pytest.fixture
def pin_name():
    return str(uuid.uuid4())


@pytest.fixture
def pin_del(board, df, pin_name):
    meta_old = board.pin_write(df, pin_name, type="csv", title="some title")
    sleep(1)
    meta_new = board.pin_write(df, pin_name, type="csv", title="some title")

    assert len(board.pin_versions(pin_name)) == 2
    assert meta_old.version.version != meta_new.version.version

    return meta_old, meta_new


@pytest.fixture
def pin_prune(board, df, pin_name):
    today = datetime.now()
    day_ago = today - timedelta(days=1, minutes=1)
    two_days_ago = today - timedelta(days=2, minutes=1)

    board.pin_write(df, pin_name, type="csv", title="some title", created=today)
    board.pin_write(df, pin_name, type="csv", title="some title", created=day_ago)
    board.pin_write(df, pin_name, type="csv", title="some title", created=two_days_ago)

    versions = board.pin_versions(pin_name, as_df=False)
    assert len(versions) == 3

    return versions


def test_board_pin_delete(board, df, pin_name, pin_del):
    board.pin_delete(pin_name)

    assert board.pin_exists(pin_name) is False


def test_board_pin_version_delete_older(board, pin_name, pin_del):
    meta_old, meta_new = pin_del

    board.pin_version_delete(pin_name, meta_old.version.version)
    df_versions = board.pin_versions(pin_name)

    # Note that using `in` on a pandas Series checks against the index :/
    assert meta_old.version.version not in df_versions.version.values
    assert meta_new.version.version in df_versions.version.values


def test_board_pin_version_delete_latest(board, pin_name, pin_del):
    meta_old, meta_new = pin_del

    if board.fs.protocol == "rsc":
        with pytest.raises(PinsError) as exc_info:
            board.pin_version_delete(pin_name, meta_new.version.version)

            "cannot delete the latest pin version" in exc_info.value.args[0]
        return

    board.pin_version_delete(pin_name, meta_new.version.version)
    df_versions = board.pin_versions(pin_name)

    # Note that using `in` on a pandas Series checks against the index :/
    assert meta_old.version.version in df_versions.version.values
    assert meta_new.version.version not in df_versions.version.values


@pytest.mark.parametrize("n", [1, 2])
def test_board_pin_versions_prune_n(board, pin_prune, pin_name, n):

    board.pin_versions_prune(pin_name, n=n)
    new_versions = board.pin_versions(pin_name, as_df=False)

    assert len(new_versions) == n

    # TODO(compat): versions are currently reversed from R pins, with latest last
    # so we need to reverse to check the n latest versions
    rev_vers = list(reversed(pin_prune))
    for ii, v in enumerate(reversed(new_versions)):
        assert rev_vers[ii].version == v.version


@pytest.mark.parametrize("days", [1, 2])
def test_board_pin_versions_prune_days(board, pin_prune, pin_name, days):

    # RStudio cannot handle days, since it involves pulling metadata
    if board.fs.protocol == "rsc":
        with pytest.raises(NotImplementedError):
            board.pin_versions_prune(pin_name, days=days)
        return

    board.pin_versions_prune(pin_name, days=days)

    new_versions = board.pin_versions(pin_name, as_df=False)

    # each of the 3 versions adds an 1 more day + 1 min
    assert len(new_versions) == days


# pin_search ==================================================================


@pytest.mark.parametrize(
    "search, matches",
    [
        # beginning character
        ("x", ["x-pin-1", "x-pin-2"]),
        # middle of name
        ("pin", ["x-pin-1", "x-pin-2", "y-pin-1"]),
        # regex set
        ("[0-9]", ["x-pin-1", "x-pin-2", "y-pin-1"]),
        # exists only in title
        ("the-title", ["x-pin-1", "x-pin-2", "y-pin-1", "y-z"]),
    ],
)
def test_board_pin_search_name(board, df, search, matches):
    if board.fs.protocol == "rsc":
        matches = ["derek/" + m for m in matches]

        # rsc doesn't search by title
        if search in ["the-title", "[0-9]"]:
            pytest.xfail()

    for name in ["x-pin-1", "x-pin-2", "y-pin-1", "y-z"]:
        board.pin_write(df, name, type="csv", title="the-title")

    metas = board.pin_search(search, as_df=False)
    sorted_meta_names = sorted([m.name for m in metas])
    assert sorted_meta_names == sorted(matches)


# BaseBoard specific ==========================================================

from pins.boards import BaseBoard  # noqa
from pins.cache import PinsCache  # noqa


def test_board_base_pin_meta_cache_touch(tmp_dir2, df):

    cache = fsspec.filesystem(
        "pinscache", target_protocol="file", same_names=True, hash_prefix=str(tmp_dir2),
    )
    board = BaseBoard(str(tmp_dir2), fs=cache)

    board.pin_write(df, "some_df", type="csv")
    meta = board.pin_meta("some_df")
    v = meta.version.version

    p_cache_version = board._get_cache_path(meta.name, v)
    p_cache_meta = Path(p_cache_version) / "data.txt"

    orig_access = p_cache_meta.stat().st_atime

    board.pin_meta("some_df")

    new_access = p_cache_meta.stat().st_atime

    assert orig_access < new_access


# RStudio Connect specific ====================================================

# import fixture that builds / tearsdown user "susan"
from pins.tests.test_rsconnect_api import (  # noqa
    fs_short,
    fs_admin,
    rsc_admin,
    rsc_short,
)
from pins.boards import BoardRsConnect  # noqa


@pytest.mark.fs_rsc
@pytest.fixture
def board_short(fs_short):  # noqa
    board_short = BoardRsConnect("", fs_short)
    return board_short


@pytest.mark.fs_rsc
def test_board_pin_write_rsc_full_name(df, board_short):  # noqa
    board_short.pin_write(df, "susan/some_df", type="csv")


@pytest.mark.fs_rsc
def test_board_pin_search_admin_user(df, board_short, fs_admin):  # noqa
    board_short.pin_write(df, "some_df", type="csv")

    board_admin = BoardRsConnect("", fs_admin)
    search_res = board_admin.pin_search("susan", as_df=False)

    assert len(search_res) == 1
    assert search_res[0].name == "susan/some_df"
    assert isinstance(search_res[0], MetaRaw)

    search_res2 = board_admin.pin_search("susan", as_df=True)
    assert search_res2.shape == (1, 6)
    assert search_res2.loc[0, "name"] == "susan/some_df"
    assert isinstance(search_res2.loc[0, "meta"], MetaRaw)


@pytest.mark.fs_rsc
def test_board_rsc_pin_write_title_update(df, board_short):
    board_short.pin_write(df, "susan/some_df", type="csv", title="title a")
    board_short.pin_write(df, "susan/some_df", type="csv", title="title b")

    content = board_short.fs.info("susan/some_df")
    assert content["title"] == "title b"


@pytest.mark.fs_rsc
def test_board_pin_meta_is_full_name(df, board_short):
    meta = board_short.pin_write(df, "susan/some_df", type="csv")

    assert meta.name == "susan/some_df"

    meta2 = board_short.pin_write(df, "some_df", type="csv")
    assert meta2.name == "susan/some_df"

    meta3 = board_short.pin_meta("some_df")
    assert meta3.name == "susan/some_df"


@pytest.mark.fs_rsc
def test_board_rsc_path_to_pin_safe(board_short):
    assert board_short.path_to_pin("me/some_pin") == "me/some_pin"


# Manual Board Specific =======================================================

from pins.boards import BoardManual  # noqa


def test_board_manual_http_file_download():
    # TODO: change when repo is moved to RStudio

    path = "https://raw.githubusercontent.com/machow/pins-python"
    license_path = "main/LICENSE"

    # use a simple cache, which automatically creates a temporary directory
    fs = fsspec.filesystem(
        "simplecache", target_protocol="http", target_options={"block_size": 0}
    )

    # with path ----
    board = BoardManual(path, fs, pin_paths={"license": "main/LICENSE"})

    assert board.pin_list() == ["license"]
    # TODO: better assert
    assert len(board.pin_download("license"))

    # no base path ----
    board2 = BoardManual("", fs, pin_paths={"license": f"{path}/{license_path}"})

    assert board2.pin_list() == ["license"]
    # TODO better assert
    assert len(board2.pin_download("license"))


def test_board_manual_pin_read():
    # TODO: block size must be set to 0 to handle gzip encoding from github
    # see https://github.com/fsspec/filesystem_spec/issues/389
    fs = fsspec.filesystem("http", block_size=0)
    board = BoardManual(
        "https://raw.githubusercontent.com/machow/pins-python/main/pins/tests/pins-compat",
        fs,
        pin_paths={
            "df_csv": "df_csv/20220214T163718Z-eceac/",
            "df_csv2_v2": "df_csv/20220214T163720Z-9bfad/",
        },
    )

    df = board.pin_read("df_csv")

    # do a somewhat data-framey check
    assert df.shape[0] > 1
