import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep

import fsspec
import pandas as pd
import pytest

# using pytest cases, so that we can pass in fixtures as parameters
# TODO: this seems like maybe overkill
from pytest_cases import fixture, parametrize

from pins.config import PINS_ENV_INSECURE_READ
from pins.errors import PinsError, PinsInsecureReadError, PinsVersionError
from pins.meta import MetaRaw
from pins.tests.helpers import DEFAULT_CREATION_DATE, rm_env, skip_if_dbc


@fixture
def df():
    import pandas as pd

    return pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})


@fixture
def board(backend):
    yield backend.create_tmp_board()
    backend.teardown()


@fixture
def board_unversioned(backend):
    yield backend.create_tmp_board(versioned=False)
    backend.teardown()


@fixture
def board_with_cache(backend):
    from pins.constructors import board as board_constructor
    from pins.constructors import board_rsconnect

    board = backend.create_tmp_board()

    if backend.fs_name == "rsc":
        # The rsconnect board is special, in that it's slower to set up and tear down,
        # so our test suite uses multiple rsconnect users in testing its API, and
        # board behavior. As a result, we need to pass the credentials directly in.
        server_url, api_key = board.fs.api.server_url, board.fs.api.api_key
        board_with_cache = board_rsconnect(server_url=server_url, api_key=api_key)
    else:
        board_with_cache = board_constructor(backend.fs_name, board.board)

    yield board_with_cache

    backend.teardown()


# misc ========================================================================


def test_board_validate_pin_name_root(board):
    with pytest.raises(ValueError) as exc_info:
        board.path_to_pin("/some_pin")

    assert "Invalid pin name" in exc_info.value.args[0]


# pin_write ===================================================================


@skip_if_dbc
def test_board_pin_write_default_title(board):
    df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
    meta = board.pin_write(df, "df_csv", title=None, type="csv")
    assert meta.title == "df_csv: a pinned 3 x 2 DataFrame"


@skip_if_dbc
def test_board_pin_write_prepare_pin(board, tmp_path: Path):
    df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})

    meta = board.prepare_pin_version(str(tmp_path), df, "df_csv", title=None, type="csv")
    assert meta.file == "df_csv.csv"
    assert (tmp_path / "data.txt").exists()
    assert (tmp_path / "df_csv.csv").exists()
    assert not (tmp_path / "df_csv.csv").is_dir()


@skip_if_dbc
def test_board_pin_write_roundtrip(board):
    df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})

    assert not board.pin_exists("df_csv")

    board.pin_write(df, "df_csv", type="csv")

    assert board.pin_exists("df_csv")

    loaded_df = board.pin_read("df_csv")
    assert loaded_df.equals(df)


@skip_if_dbc
def test_board_pin_write_type_not_specified_error(board):
    class C:
        pass

    with pytest.raises(NotImplementedError):
        board.pin_write(C(), "cool_pin")


@skip_if_dbc
def test_board_pin_write_type_error(board):
    class C:
        pass

    with pytest.raises(NotImplementedError) as exc_info:
        board.pin_write(C(), "cool_pin", type="MY_TYPE")

    assert "MY_TYPE" in exc_info.value.args[0]


@skip_if_dbc
def test_board_pin_write_feather_deprecated(board):
    df = pd.DataFrame({"x": [1, 2, 3]})

    with pytest.warns(DeprecationWarning):
        board.pin_write(df, "cool_pin", type="feather")


@skip_if_dbc
def test_board_pin_write_file_raises_error(board, tmp_path):
    df = pd.DataFrame({"x": [1, 2, 3]})

    path = tmp_path.joinpath("data.csv")
    df.to_csv(path, index=False)

    # TODO: should this error?
    with pytest.raises(NotImplementedError):
        board.pin_write(path, "cool_pin", type="file")


@skip_if_dbc
@pytest.mark.parametrize("force_identical_write", [True, False])
def test_board_pin_write_force_identical_write_pincount(board, force_identical_write):
    df = pd.DataFrame({"x": [1, 2, 3]})

    # 1min ago to avoid name collision
    one_min_ago = datetime.now() - timedelta(minutes=1)
    board.pin_write(df, "cool_pin", type="csv", created=one_min_ago)
    board.pin_write(
        df, "cool_pin", type="csv", force_identical_write=force_identical_write
    )
    versions = board.pin_versions("cool_pin")
    if force_identical_write:
        assert len(versions) == 2
    else:
        assert len(versions) == 1


@skip_if_dbc
def test_board_pin_write_force_identical_write_msg(
    board, capfd: pytest.CaptureFixture[str]
):
    df = pd.DataFrame({"x": [1, 2, 3]})

    # 1min ago to avoid name collision
    one_min_ago = datetime.now() - timedelta(minutes=1)
    board.pin_write(df, "cool_pin", type="csv", created=one_min_ago)
    board.pin_write(df, "cool_pin", type="csv")
    versions = board.pin_versions("cool_pin")

    _, err = capfd.readouterr()
    msg = 'The hash of pin "cool_pin" has not changed. Your pin will not be stored.'
    assert msg in err
    assert len(versions) == 1


@skip_if_dbc
def test_board_pin_download(board_with_cache, tmp_path):
    # create and save data
    df = pd.DataFrame({"x": [1, 2, 3]})

    path = tmp_path / "data.csv"
    df.to_csv(path, index=False)

    meta = board_with_cache.pin_upload(path, "cool_pin")
    assert meta.type == "file"

    (pin_path,) = board_with_cache.pin_download("cool_pin")
    df = pd.read_csv(pin_path)
    assert df.x.tolist() == [1, 2, 3]

    with pytest.raises(NotImplementedError):
        board_with_cache.pin_read("cool_pin")


@skip_if_dbc
def test_board_pin_download_filename_many_suffixes(board_with_cache, tmp_path):
    # create and save data
    df = pd.DataFrame({"x": [1, 2, 3]})

    path = tmp_path / "data.a.b.csv"
    df.to_csv(path, index=False)

    board_with_cache.pin_upload(path, "cool_pin")

    (pin_path,) = board_with_cache.pin_download("cool_pin")
    assert Path(pin_path).name == "data.a.b.csv"

    df = pd.read_csv(pin_path)
    assert df.x.tolist() == [1, 2, 3]


@skip_if_dbc
def test_board_pin_download_filename_no_suffixes(board_with_cache, tmp_path):
    # create and save data
    df = pd.DataFrame({"x": [1, 2, 3]})

    path = tmp_path / "data"
    df.to_csv(path, index=False)

    board_with_cache.pin_upload(path, "cool_pin")

    (pin_path,) = board_with_cache.pin_download("cool_pin")
    assert Path(pin_path).name == "data"

    df = pd.read_csv(pin_path)
    assert df.x.tolist() == [1, 2, 3]


@skip_if_dbc
def test_board_pin_download_filename(board_with_cache, tmp_path):
    # create and save data
    df = pd.DataFrame({"x": [1, 2, 3]})

    path = tmp_path / "data.csv"
    df.to_csv(path, index=False)

    meta = board_with_cache.pin_upload(path, "cool_pin")

    assert meta.file == "data.csv"

    (pin_path,) = board_with_cache.pin_download("cool_pin")
    assert Path(pin_path).name == "data.csv"


@skip_if_dbc
def test_board_pin_download_no_cache_error(board, tmp_path):
    df = pd.DataFrame({"x": [1, 2, 3]})
    path = tmp_path / "data.csv"
    df.to_csv(path, index=False)

    # TODO: should this error?
    meta = board.pin_upload(path, "cool_pin")
    assert meta.type == "file"

    # file boards work okay, since the board directory itself is the cache
    if board.fs.protocol in ["file", ("file", "local")]:
        pytest.skip()

    # uncached boards should fail, since nowhere to store the download
    with pytest.raises(PinsError):
        (pin_path,) = board.pin_download("cool_pin")


@skip_if_dbc
def test_board_pin_upload_path_list(board_with_cache, tmp_path):
    # create and save data
    df = pd.DataFrame({"x": [1, 2, 3]})

    path = tmp_path / "data.csv"
    df.to_csv(path, index=False)

    meta = board_with_cache.pin_upload([path], "cool_pin")
    assert meta.type == "file"

    (pin_path,) = board_with_cache.pin_download("cool_pin")


@skip_if_dbc
def test_board_pin_download_filename_multifile(board_with_cache, tmp_path):
    # create and save data
    df = pd.DataFrame({"x": [1, 2, 3]})

    path1, path2 = tmp_path / "data1.csv", tmp_path / "data2.csv"
    df.to_csv(path1, index=False)
    df.to_csv(path2, index=False)

    print(board_with_cache.fs.protocol)
    meta = board_with_cache.pin_upload([path1, path2], "cool_pin")

    assert meta.type == "file"
    assert meta.file == ["data1.csv", "data2.csv"]

    pin_path = board_with_cache.pin_download("cool_pin")

    assert len(pin_path) == 2
    assert Path(pin_path[0]).name == "data1.csv"
    assert Path(pin_path[1]).name == "data2.csv"


def test_board_pin_write_rsc_index_html(board, tmp_path: Path, snapshot):
    if board.fs.protocol != "rsc":
        pytest.skip()

    df = pd.DataFrame({"x": [1, 2, None], "y": ["a", "b", "c"]})

    pin_name = "test_rsc_pin"

    board.prepare_pin_version(
        str(tmp_path),
        df,
        pin_name,
        type="csv",
        title="some pin",
        description="some description",
        created=DEFAULT_CREATION_DATE,
    )

    snapshot.assert_equal_dir(tmp_path)


# pin_write against different types -------------------------------------------


@skip_if_dbc
@parametrize(
    "obj, type_",
    [
        (df, "csv"),
        (df, "joblib"),
        ({"a": 1, "b": [2, 3]}, "joblib"),
        ({"a": 1, "b": [2, 3]}, "json"),
    ],
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


@skip_if_dbc
def test_board_pin_read_insecure_fail_default(board):
    board.pin_write({"a": 1}, "test_pin", type="joblib", title="some title")
    with pytest.raises(PinsInsecureReadError) as exc_info:
        board.pin_read("test_pin")

    assert "joblib" in exc_info.value.args[0]


@skip_if_dbc
def test_board_pin_read_insecure_fail_board_flag(board):
    # board flag prioritized over env var
    with rm_env(PINS_ENV_INSECURE_READ):
        os.environ[PINS_ENV_INSECURE_READ] = "1"
        board.allow_pickle_read = False
        board.pin_write({"a": 1}, "test_pin", type="joblib", title="some title")
        with pytest.raises(PinsInsecureReadError):
            board.pin_read("test_pin")


@skip_if_dbc
def test_board_pin_read_insecure_succeed_board_flag(board):
    # board flag prioritized over env var
    with rm_env(PINS_ENV_INSECURE_READ):
        os.environ[PINS_ENV_INSECURE_READ] = "0"
        board.allow_pickle_read = True
        board.pin_write({"a": 1}, "test_pin", type="joblib", title="some title")
        board.pin_read("test_pin")


# pin_write with unversioned boards ===========================================


@skip_if_dbc
@pytest.mark.parametrize("versioned", [None, False])
def test_board_unversioned_pin_write_unversioned_force_identical_write(
    versioned, board_unversioned
):
    # 1min ago to avoid name collision
    one_min_ago = datetime.now() - timedelta(minutes=1)
    board_unversioned.pin_write(
        {"a": 1},
        "test_pin",
        type="json",
        versioned=versioned,
        created=one_min_ago,
        force_identical_write=True,
    )
    board_unversioned.pin_write(
        {"a": 2},
        "test_pin",
        type="json",
        versioned=versioned,
        force_identical_write=True,
    )

    assert len(board_unversioned.pin_versions("test_pin")) == 1
    assert board_unversioned.pin_read("test_pin") == {"a": 2}


@skip_if_dbc
@pytest.mark.parametrize("versioned", [None, False])
def test_board_unversioned_pin_write_unversioned(versioned, board_unversioned):
    board_unversioned.pin_write({"a": 1}, "test_pin", type="json", versioned=versioned)
    board_unversioned.pin_write({"a": 2}, "test_pin", type="json", versioned=versioned)

    assert len(board_unversioned.pin_versions("test_pin")) == 1
    assert board_unversioned.pin_read("test_pin") == {"a": 2}


@skip_if_dbc
def test_board_unversioned_pin_write_versioned(board_unversioned):
    board_unversioned.pin_write({"a": 1}, "test_pin", type="json", versioned=False)
    board_unversioned.pin_write({"a": 2}, "test_pin", type="json", versioned=True)

    assert len(board_unversioned.pin_versions("test_pin")) == 2


@skip_if_dbc
def test_board_versioned_pin_write_unversioned(board):
    # should fall back to the versioned setting of the board
    board.pin_write({"a": 1}, "test_pin", type="json")
    board.pin_write({"a": 2}, "test_pin", type="json")

    with pytest.raises(PinsVersionError):
        board.pin_write({"a": 3}, "test_pin", type="json", versioned=False)

    assert len(board.pin_versions("test_pin")) == 2


# pin_delete ==================================================================


@pytest.fixture
def pin_name():
    return str(uuid.uuid4())


@pytest.fixture
def pin_del(board, df, pin_name):
    # TODO: update when dbc boards no longer read-only
    if board.fs.protocol == "dbc":
        pytest.skip()
    # 1min ago to avoid name collision
    one_min_ago = datetime.now() - timedelta(minutes=1)
    meta_old = board.pin_write(
        df, pin_name, type="csv", title="some title", created=one_min_ago
    )
    meta_new = board.pin_write(
        df, pin_name, type="csv", title="some title", force_identical_write=True
    )

    assert len(board.pin_versions(pin_name)) == 2
    assert meta_old.version.version != meta_new.version.version

    return meta_old, meta_new


@pytest.fixture
def pin_prune(board, df, pin_name):
    # TODO: update when dbc boards no longer read-only
    if board.fs.protocol == "dbc":
        pytest.skip()
    today = datetime.now()
    day_ago = today - timedelta(days=1, minutes=1)
    two_days_ago = today - timedelta(days=2, minutes=1)

    board.pin_write(df, pin_name, type="csv", title="some title", created=today)
    board.pin_write(
        df,
        pin_name,
        type="csv",
        title="some title",
        created=day_ago,
        force_identical_write=True,
    )
    board.pin_write(
        df,
        pin_name,
        type="csv",
        title="some title",
        created=two_days_ago,
        force_identical_write=True,
    )

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
    # Posit cannot handle days, since it involves pulling metadata
    if board.fs.protocol == "rsc":
        with pytest.raises(NotImplementedError):
            board.pin_versions_prune(pin_name, days=days)
        return

    board.pin_versions_prune(pin_name, days=days)

    new_versions = board.pin_versions(pin_name, as_df=False)

    # each of the 3 versions adds an 1 more day + 1 min
    assert len(new_versions) == days


@skip_if_dbc
def test_board_pin_versions_prune_days_protect_most_recent(board, pin_name):
    """To address https://github.com/rstudio/pins-python/issues/297"""
    # Posit cannot handle days, since it involves pulling metadata
    if board.fs.protocol == "rsc":
        with pytest.raises(NotImplementedError):
            board.pin_versions_prune(pin_name, days=5)
        return

    today = datetime.now()
    two_days_ago = today - timedelta(days=2, minutes=1)
    three_days_ago = today - timedelta(days=3, minutes=1)

    # Note, we are _not_ going to write a pin for today, otherwise we wouldn't be
    # properly testing the protection of the most recent version - it would be trivially
    # protected because it would always lie in the last day / fraction of a day.
    board.pin_write({"a": 1}, pin_name, type="json", created=two_days_ago)
    assert len(board.pin_versions(pin_name, as_df=False)) == 1
    board.pin_write({"a": 2}, pin_name, type="json", created=three_days_ago)

    # prune the versions, keeping only the most recent
    board.pin_versions_prune(pin_name, days=1)

    # check that only the most recent version remains
    versions = board.pin_versions(pin_name, as_df=False)
    assert len(versions) == 1


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
@skip_if_dbc
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


def test_board_base_pin_meta_cache_touch(tmp_path: Path, df):
    cache = fsspec.filesystem(
        "pinscache",
        target_protocol="file",
        same_names=True,
        hash_prefix=str(tmp_path),
    )
    board = BaseBoard(str(tmp_path), fs=cache)

    board.pin_write(df, "some_df", type="csv")
    meta = board.pin_meta("some_df")
    v = meta.version.version

    p_cache_version = board._get_cache_path(meta.name, v)
    p_cache_meta = Path(p_cache_version) / "data.txt"

    orig_access = p_cache_meta.stat().st_atime

    sleep(0.3)

    board.pin_meta("some_df")

    new_access = p_cache_meta.stat().st_atime

    assert orig_access < new_access


# Posit Connect specific ====================================================

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
    board_short.pin_write(
        df, "susan/some_df", type="csv", title="title b", force_identical_write=True
    )

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


@pytest.mark.fs_rsc
def test_board_rsc_require_full_pin_name(board_short):
    # the tests set the special env var below to allow short pin names,
    # but here we test this is not what should happen by default.
    # we test that full names work with the RSC board above, so these two
    # things should cover us.
    from pins.config import PINS_ENV_ALLOW_RSC_SHORT_NAME

    with rm_env(PINS_ENV_ALLOW_RSC_SHORT_NAME):
        with pytest.raises(ValueError) as exc_info:
            board_short.validate_pin_name("mtcars")

        assert "Invalid pin name" in exc_info.value.args[0]

        with pytest.raises(ValueError) as exc_info:
            board_short.path_to_pin("mtcars")

        assert "Invalid pin name" in exc_info.value.args[0]


@pytest.mark.fs_rsc
def test_board_rsc_pin_write_other_user_fails(df, board_short):
    with pytest.raises(PinsError) as exc_info:
        board_short.pin_write(df, "derek/mtcarszzzzz")

    assert "a new piece of content for another user" in exc_info.value.args[0]


@pytest.mark.fs_rsc
def test_board_rsc_pin_write_acl(df, board_short):
    board_short.pin_write(df, "susan/mtcars", type="csv", access_type="all")
    content = board_short.fs.info("susan/mtcars")
    assert content["access_type"] == "all"


@pytest.mark.fs_rsc
def test_board_rsc_pin_read_public(df, board_short):
    from pins.boards import BoardManual

    board_short.pin_write(df, "susan/mtcars", type="csv", access_type="all")

    # note that users can also get this from the web ui
    content_url = board_short.fs.info("susan/mtcars")["content_url"]

    # shouldn't be a key set in env, but remove just in case
    fs = fsspec.filesystem("http")
    board_url = BoardManual("", fs, pin_paths={"rsc_public": content_url})

    df_no_key = board_url.pin_read("rsc_public")
    assert df_no_key.equals(df)


# Manual Board Specific =======================================================

from pins.boards import BoardManual  # noqa


def test_board_manual_http_file_download():
    path = "https://raw.githubusercontent.com/rstudio/pins-python"
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
        "https://raw.githubusercontent.com/rstudio/pins-python/main/pins/tests/pins-compat",
        fs,
        pin_paths={
            "df_csv": "df_csv/20220214T163718Z-eceac/",
            "df_csv2_v2": "df_csv/20220214T163720Z-9bfad/",
        },
    )

    df = board.pin_read("df_csv")

    # do a somewhat data-framey check
    assert df.shape[0] > 1


def test_board_manual_construct_path():
    fs = fsspec.filesystem("file")
    root = "pins/tests/pins-compat"
    path_df_csv = "df_csv/20220214T163718Z-eceac/"
    path_df_csv_v2 = "df_csv/20220214T163720Z-9bfad/df_csv.csv"

    board = BoardManual(
        root,
        fs,
        pin_paths={
            "df_csv": path_df_csv,
            "df_csv2_v2": path_df_csv_v2,
        },
    )

    # path to pin folder ----
    # creates path to pin, ignores version, can include data.txt
    assert board.construct_path(["df_csv"]) == f"{root}/{path_df_csv}"
    assert board.construct_path(["df_csv", "v"]) == f"{root}/{path_df_csv}"
    assert (
        board.construct_path(["df_csv", "v", "data.txt"])
        == f"{root}/{path_df_csv}data.txt"
    )

    with pytest.raises(NotImplementedError) as exc_info:
        board.construct_path(["df_csv", "v", "data.txt", "too_much"])

    assert "Unable to construct path" in exc_info.value.args[0]

    # path to individual file ----
    assert board.construct_path(["df_csv2_v2"]) == f"{root}/{path_df_csv_v2}"

    with pytest.raises(ValueError) as exc_info:
        board.construct_path(["df_csv2_v2", "v"])

    assert "assumed to be a single file" in exc_info.value.args[0]
