from __future__ import annotations

from pathlib import Path

import fsspec
import geopandas as gpd
import pandas as pd
import pytest

from pins._adaptors import create_adaptor
from pins.config import PINS_ENV_INSECURE_READ
from pins.drivers import default_title, load_data, load_path, save_data
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
        (
            gpd.GeoDataFrame({"x": [1], "geometry": [None]}),
            "somename: a pinned 1 x 2 GeoDataFrame",
        ),
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


def test_driver_geoparquet_roundtrip(tmp_path):
    import geopandas as gpd

    gdf = gpd.GeoDataFrame(
        {"x": [1, 2, 3], "geometry": gpd.points_from_xy([1, 2, 3], [1, 2, 3])}
    )

    fname = "some_gdf"
    full_file = f"{fname}.parquet"

    p_obj = tmp_path / fname
    res_fname = save_data(gdf, p_obj, "geoparquet")

    assert Path(res_fname).name == full_file

    meta = MetaRaw(full_file, "geoparquet", "my_pin")
    obj = load_data(meta, fsspec.filesystem("file"), tmp_path, allow_pickle_read=True)

    assert gdf.equals(obj)


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


class TestSaveData:
    def test_accepts_pandas_df(self, tmp_path: Path):
        import pandas as pd

        df = pd.DataFrame({"x": [1, 2, 3]})
        result = save_data(df, tmp_path / "some_df", "csv")
        assert Path(result) == tmp_path / "some_df.csv"

    def test_accepts_adaptor(self, tmp_path: Path):
        import pandas as pd

        df = pd.DataFrame({"x": [1, 2, 3]})
        adaptor = create_adaptor(df)
        result = save_data(adaptor, tmp_path / "some_df", "csv")
        assert Path(result) == tmp_path / "some_df.csv"


class TestLoadFile:
    def test_str_file(self):
        class _MockMetaStrFile:
            file: str = "a"
            type: str = "csv"

        assert load_path(_MockMetaStrFile().file, None, _MockMetaStrFile().type) == "a"

    def test_table(self):
        class _MockMetaTable:
            file: str = "a"
            type: str = "table"

        assert load_path(_MockMetaTable().file, None, _MockMetaTable().type) == "data.csv"

    def test_version(self):
        class _MockMetaTable:
            file: str = "a"
            type: str = "csv"

        assert load_path(_MockMetaTable().file, "v1", _MockMetaTable().type) == "v1/a"
