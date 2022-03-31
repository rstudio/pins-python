import contextlib
import os
import pytest

from pathlib import Path

from pins import constructors as c

# adapted from https://stackoverflow.com/a/34333710


@contextlib.contextmanager
def rm_env(*args):
    """
    Temporarily set the process environment variables.


    """
    old_environ = dict(os.environ)
    for arg in args:
        if arg in os.environ:
            del os.environ[arg]

    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(old_environ)


def check_dir_writable(p_dir):
    assert p_dir.parent.exists()
    assert os.access(p_dir.parent.absolute(), os.W_OK)


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
