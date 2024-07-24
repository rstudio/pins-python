import pandas as pd
from importlib_resources import files

from pins import board_folder

OLD_BOARD = files("pins") / "tests" / "pins-old-types"
DST_DF = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})


def test_compat_old_types_load_table():
    board = board_folder(OLD_BOARD)
    src_df = board.pin_read("a-table")

    assert isinstance(src_df, pd.DataFrame)
    assert src_df.equals(DST_DF)
