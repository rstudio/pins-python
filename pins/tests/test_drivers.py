from pathlib import Path

import fsspec
import pandas as pd
import polars as pl
import pytest

from pins.config import PINS_ENV_INSECURE_READ
from pins.drivers import _choose_df_lib, default_title, load_data, save_data
from pins.errors import PinsInsecureReadError
from pins.meta import MetaRaw
from pins.tests.helpers import rm_env


@pytest.fixture
def some_joblib(tmp_path: Path):
    import joblib

    p_obj = tmp_path / "some.joblib"
    joblib.dump({"a": 1}, p_obj)

    return p_obj


# default title ---------------------------------------------------------------


class ExC:
    class D:
        pass


@pytest.mark.parametrize(
    "obj, dst_title",
    [
        (pd.DataFrame({"x": [1, 2]}), "somename: a pinned 2 x 1 DataFrame"),
        (pd.DataFrame({"x": [1], "y": [2]}), "somename: a pinned 1 x 2 DataFrame"),
        (pl.DataFrame({"x": [1, 2]}), "somename: a pinned 2 x 1 DataFrame"),
        (ExC(), "somename: a pinned ExC object"),
        (ExC().D(), "somename: a pinned ExC.D object"),
        ([1, 2, 3], "somename: a pinned list object"),
    ],
)
def test_default_title(obj, dst_title):
    res = default_title(obj, "somename")
    assert res == dst_title


@pytest.mark.parametrize(
    "type_",
    [
        "csv",
        "arrow",
        "parquet",
        "joblib",
    ],
)
def test_driver_roundtrip(tmp_path: Path, type_):
    # TODO: I think this test highlights the challenge of getting the flow
    # between metadata, drivers, and the metafactory right.
    # There is the name of the data (relative to the pin directory), and the full
    # name of data in its temporary directory.
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3]})

    fname = "some_df"
    full_file = f"{fname}.{type_}"

    p_obj = tmp_path / fname
    res_fname = save_data(df, p_obj, type_)

    assert Path(res_fname).name == full_file

    meta = MetaRaw(full_file, type_, "my_pin")
    obj = load_data(meta, fsspec.filesystem("file"), tmp_path, allow_pickle_read=True)

    assert df.equals(obj)


@pytest.mark.parametrize(
    "type_",
    [
        "parquet",
    ],
)
def test_driver_polars_roundtrip(tmp_path, type_):
    import polars as pl

    df = pl.DataFrame({"x": [1, 2, 3]})

    fname = "some_df"
    full_file = f"{fname}.{type_}"

    p_obj = tmp_path / fname
    res_fname = save_data(df, p_obj, type_)

    assert Path(res_fname).name == full_file

    meta = MetaRaw(full_file, type_, "my_pin")
    pandas_df = load_data(
        meta, fsspec.filesystem("file"), tmp_path, allow_pickle_read=True
    )

    # Convert from pandas to polars
    obj = pl.DataFrame(pandas_df)

    assert df.equals(obj)


@pytest.mark.parametrize(
    "type_",
    [
        "json",
    ],
)
def test_driver_roundtrip_json(tmp_path: Path, type_):
    df = {"x": [1, 2, 3]}

    fname = "some_df"
    full_file = f"{fname}.{type_}"

    p_obj = tmp_path / fname
    res_fname = save_data(df, p_obj, type_)

    assert Path(res_fname).name == full_file

    meta = MetaRaw(full_file, type_, "my_pin")
    obj = load_data(meta, fsspec.filesystem("file"), tmp_path, allow_pickle_read=True)

    assert df == obj


def test_driver_feather_write_error(tmp_path: Path):
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3]})

    fname = "some_df"

    p_obj = tmp_path / fname

    with pytest.raises(NotImplementedError) as exc_info:
        save_data(df, p_obj, "feather")

    assert '"feather" no longer supported.' in exc_info.value.args[0]


def test_driver_feather_read_backwards_compat(tmp_path: Path):
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3]})

    fname = "some_df"
    full_file = f"{fname}.feather"

    df.to_feather(tmp_path / full_file)

    obj = load_data(
        MetaRaw(full_file, "feather", "my_pin"), fsspec.filesystem("file"), tmp_path
    )

    assert df.equals(obj)


def test_driver_pickle_read_fail_explicit(some_joblib):
    meta = MetaRaw(some_joblib.name, "joblib", "my_pin")
    with pytest.raises(PinsInsecureReadError):
        load_data(
            meta, fsspec.filesystem("file"), some_joblib.parent, allow_pickle_read=False
        )


def test_driver_pickle_read_fail_default(some_joblib):
    meta = MetaRaw(some_joblib.name, "joblib", "my_pin")
    with rm_env(PINS_ENV_INSECURE_READ), pytest.raises(PinsInsecureReadError):
        load_data(
            meta, fsspec.filesystem("file"), some_joblib.parent, allow_pickle_read=False
        )


def test_driver_apply_suffix_false(tmp_path: Path):
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3]})

    fname = "some_df"
    type_ = "csv"

    p_obj = tmp_path / fname
    res_fname = save_data(df, p_obj, type_, apply_suffix=False)

    assert Path(res_fname).name == "some_df"


class TestChooseDFLib:
    def test_pandas(self):
        assert _choose_df_lib(pd.DataFrame({"x": [1]})) == "pandas"

    def test_polars(self):
        assert _choose_df_lib(pl.DataFrame({"x": [1]})) == "polars"

    def test_list_raises(self):
        with pytest.raises(
            NotImplementedError, match="Unrecognized DataFrame type: <class 'list'>"
        ):
            _choose_df_lib([])

    def test_pandas_subclass(self):
        class MyDataFrame(pd.DataFrame):
            pass

        assert _choose_df_lib(MyDataFrame({"x": [1]})) == "pandas"

    def test_ftype_compatible(self):
        assert (
            _choose_df_lib(
                pd.DataFrame({"x": [1]}), supported_libs=["pandas"], file_type="csv"
            )
            == "pandas"
        )

    def test_ftype_incompatible(self):
        with pytest.raises(
            NotImplementedError,
            match=(
                "Currently only pandas DataFrames can be saved for type 'csv'. "
                "DataFrames from polars are not yet supported."
            ),
        ):
            _choose_df_lib(
                pl.DataFrame({"x": [1]}), supported_libs=["pandas"], file_type="csv"
            )

    def test_supported_alone_raises(self):
        with pytest.raises(
            ValueError,
            match="Must provide both or neither of supported_libs and file_type",
        ):
            _choose_df_lib(..., supported_libs=["pandas"])

    def test_file_type_alone_raises(self):
        with pytest.raises(
            ValueError,
            match="Must provide both or neither of supported_libs and file_type",
        ):
            _choose_df_lib(..., file_type="csv")
