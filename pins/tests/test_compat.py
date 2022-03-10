import pytest
import datetime

import importlib_resources as resources

from pins.errors import PinsError
from pins.tests.helpers import xfail_fs


NOT_A_PIN = "not_a_pin_abcdefg"
PIN_CSV = "df_csv"

path_to_board = resources.files("pins") / "tests/pins-compat"


# set up board ----


@pytest.fixture(scope="session")
def board(backend):
    board = backend.create_tmp_board(str(path_to_board.absolute()))

    yield board

    backend.teardown_board(board)


# pin_list --------------------------------------------------------------------


def test_compat_pin_list(board):
    src_sorted = sorted(board.pin_list())
    dst_sorted = ["df_arrow", "df_csv", "df_rds", "df_unversioned"]

    if board.fs.protocol == "rsc":
        # rsc backend uses <user_name>/<content_name> for full name
        dst_sorted = [f"{board.user_name}/{content}" for content in dst_sorted]

    assert src_sorted == dst_sorted


# pin_exists --------------------------------------------------------------------


def test_compat_pin_exists_succeed(board):
    assert board.pin_exists(PIN_CSV)


def test_compat_pin_exists_fails(board):
    assert board.pin_exists(NOT_A_PIN) is False


# pin_meta --------------------------------------------------------------------


def test_compat_pin_meta(board):
    # Note that this fetches the latest of 2 versions
    meta = board.pin_meta(PIN_CSV)

    if board.fs.protocol == "rsc":
        # TODO: afaik the bundle id is largely non-deterministic, so not possible
        # to test, but should think a bit more about it.
        pass
    else:
        assert meta.version.version == "20220214T163720Z-9bfad"
        assert meta.version.created == datetime.datetime(2022, 2, 14, 16, 37, 20)
        assert meta.version.hash == "9bfad"

    assert meta.title == "df_csv: a pinned 2 x 3 data frame"
    assert meta.description is None
    assert meta.created == "20220214T163720Z"
    assert meta.file == "df_csv.csv"
    assert meta.file_size == 28
    assert meta.pin_hash == "9bfad6d1a322a904"
    assert meta.type == "csv"

    # TODO(question): coding api_version as a yaml float intentional?
    assert meta.api_version == 1.0
    assert meta.name is None
    assert meta.user == {}


def test_compat_pin_meta_pin_missing(board):
    with pytest.raises(PinsError) as exc_info:
        board.pin_meta(NOT_A_PIN)

    assert f"{NOT_A_PIN} does not exist" in exc_info.value.args[0]


@xfail_fs("rsc")
def test_compat_pin_meta_version_arg(board):
    # note that in RSConnect the version is the bundle id
    meta = board.pin_meta(PIN_CSV, "20220214T163718Z-eceac")
    assert meta.version.version == "20220214T163718Z-eceac"
    assert meta.version.hash == "eceac"


def test_compat_pin_meta_version_arg_error(board):
    bad_version = "123"
    with pytest.raises(PinsError) as exc_info:
        board.pin_meta(PIN_CSV, bad_version)

    msg = exc_info.value.args[0]
    assert PIN_CSV in msg
    assert bad_version in msg


# pin_read ----


def test_compat_pin_read(board):
    import pandas as pd

    p_data = path_to_board / "df_csv" / "20220214T163720Z-9bfad" / "df_csv.csv"

    src_df = board.pin_read("df_csv")
    dst_df = pd.read_csv(p_data, index_col=0)

    assert isinstance(src_df, pd.DataFrame)
    assert src_df.equals(dst_df)


def test_compat_pin_read_supported(board):
    with pytest.raises(NotImplementedError):
        board.pin_read("df_rds")


# pin_write ----
