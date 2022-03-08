import pytest

from pathlib import Path


@pytest.fixture
def board(backend):
    yield backend.create_tmp_board()
    backend.teardown()


def test_board_pin_write_default_title(board):
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
    meta = board.pin_write(df, "df_csv", title=None, type="csv")
    assert meta.title == "A pinned 3 x 2 CSV"


def test_board_pin_write_prepare_pin(board, tmp_dir2):
    import pandas as pd

    p_tmp = Path(tmp_dir2)
    df = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})

    meta = board.prepare_pin_version(tmp_dir2, df, "df_csv", title=None, type="csv")
    assert meta.file == "df_csv"
    assert (p_tmp / "data.txt").exists()
    assert (p_tmp / "df_csv").exists()
    assert not (p_tmp / "df_csv").is_dir()


def test_board_pin_write_roundtrip(board):
    import pandas as pd

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
