import pytest
import datetime


from pins.errors import PinsError
from pins.tests.helpers import xfail_fs
from pins.tests.conftest import PATH_TO_EXAMPLE_BOARD, PATH_TO_MANIFEST_BOARD


NOT_A_PIN = "not_a_pin_abcdefg"
PIN_CSV = "df_csv"

# set up board ----


@pytest.fixture(scope="session")
def board(backend):
    board = backend.create_tmp_board(str(PATH_TO_EXAMPLE_BOARD.absolute()))

    yield board

    backend.teardown_board(board)


@pytest.fixture(scope="session")
def board_manifest(backend):
    board = backend.create_tmp_board(str(PATH_TO_MANIFEST_BOARD.absolute()))

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


# pin_versions ----------------------------------------------------------------


def test_compat_pin_versions(board):
    if board.fs.protocol == "rsc":
        pytest.skip("RSC uses bundle ids as pin versions")
    versions = board.pin_versions("df_csv", as_df=False)
    v_strings = list(v.version for v in versions)
    assert v_strings == ["20220214T163718Z-eceac", "20220214T163720Z-9bfad"]


@pytest.mark.skip("Used to diagnose os listdir ordering")
def test_compat_os_listdir():
    import os

    res = os.listdir(PATH_TO_EXAMPLE_BOARD / "df_csv")
    dst = ["20220214T163718Z-eceac", "20220214T163720Z-9bfad"]

    assert res == dst


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
        assert meta.name == "derek/df_csv"
    else:
        assert meta.version.version == "20220214T163720Z-9bfad"
        assert meta.version.created == datetime.datetime(2022, 2, 14, 16, 37, 20)
        assert meta.version.hash == "9bfad"

        assert meta.name == "df_csv"

    assert meta.title == "df_csv: a pinned 2 x 3 data frame"
    assert meta.description is None
    assert meta.created == "20220214T163720Z"
    assert meta.file == "df_csv.csv"
    assert meta.file_size == 28
    assert meta.pin_hash == "9bfad6d1a322a904"
    assert meta.type == "csv"

    # TODO(question): coding api_version as a yaml float intentional?
    assert meta.api_version == 1.0
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

    p_data = PATH_TO_EXAMPLE_BOARD / "df_csv" / "20220214T163720Z-9bfad" / "df_csv.csv"

    src_df = board.pin_read("df_csv")
    dst_df = pd.read_csv(p_data)

    assert isinstance(src_df, pd.DataFrame)
    assert src_df.equals(dst_df)


def test_compat_pin_read_supported(board):
    with pytest.raises(NotImplementedError):
        board.pin_read("df_rds")


# pin_write ----

# manifest -----


def test_board_pin_write_manifest_name_error(board_manifest):
    if board_manifest.fs.protocol == "rsc":
        pytest.skip()

    with pytest.raises(ValueError) as exc_info:
        board_manifest.pin_write([1], "_pins.yaml", type="json")

    assert "name '_pins.yaml' is reserved for internal use." in exc_info.value.args[0]


def test_board_manifest_pin_list_no_internal_name(board_manifest):
    assert board_manifest.pin_list() == ["x", "y"]


def test_board_manifest_pin_exist_internal_name_errors(board_manifest):
    with pytest.raises(ValueError) as exc_info:
        board_manifest.pin_exists("_pins.yaml")

    assert "reserved for internal use." in exc_info.value.args[0]


def test_board_manifest_pin_read_internal_errors(board_manifest):
    with pytest.raises(ValueError) as exc_info:
        board_manifest.pin_read("_pins.yaml")

    assert "reserved for internal use." in exc_info.value.args[0]


def test_board_manifest_pin_search(board_manifest):
    res = board_manifest.pin_search("x", as_df=False)

    assert len(res) == 1
    assert res[0].name == "x"
