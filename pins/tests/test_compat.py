import fsspec
import pytest

import importlib_resources as resources
from pins.boards import BaseBoard

path_to_board = resources.files("pins") / "tests/pins-compat"

# set up board ----


@pytest.fixture
def board():
    fs = fsspec.filesystem("file")
    board = BaseBoard(str(path_to_board.absolute()), fs=fs)
    return board


def test_compat_pin_list(board):
    src_sorted = sorted(board.pin_list())
    dst_sorted = sorted(["df_arrow", "df_csv", "df_rds", "df_unversioned"])
    assert src_sorted == dst_sorted
