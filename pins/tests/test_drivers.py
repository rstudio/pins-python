import fsspec
import pytest
import pandas as pd

from pathlib import Path

from pins.tests.helpers import rm_env

from pins.meta import MetaRaw
from pins.config import PINS_ENV_INSECURE_READ
from pins.drivers import load_data, save_data, default_title
from pins.errors import PinsInsecureReadError


@pytest.fixture
def some_joblib(tmp_dir2):
    import joblib

    p_obj = tmp_dir2 / "some.joblib"
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
def test_driver_roundtrip(tmp_dir2, type_):
    # TODO: I think this test highlights the challenge of getting the flow
    # between metadata, drivers, and the metafactory right.
    # There is the name of the data (relative to the pin directory), and the full
    # name of data in its temporary directory.
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3]})

    fname = "some_df"
    full_file = f"{fname}.{type_}"

    p_obj = tmp_dir2 / fname
    res_fname = save_data(df, p_obj, type_)

    assert Path(res_fname).name == full_file

    meta = MetaRaw(full_file, type_, "my_pin")
    obj = load_data(meta, fsspec.filesystem("file"), tmp_dir2, allow_pickle_read=True)

    assert df.equals(obj)


@pytest.mark.parametrize(
    "type_",
    [
        "json",
    ],
)
def test_driver_roundtrip_json(tmp_dir2, type_):
    df = {"x": [1, 2, 3]}

    fname = "some_df"
    full_file = f"{fname}.{type_}"

    p_obj = tmp_dir2 / fname
    res_fname = save_data(df, p_obj, type_)

    assert Path(res_fname).name == full_file

    meta = MetaRaw(full_file, type_, "my_pin")
    obj = load_data(meta, fsspec.filesystem("file"), tmp_dir2, allow_pickle_read=True)

    assert df == obj


def test_driver_feather_write_error(tmp_dir2):
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3]})

    fname = "some_df"

    p_obj = tmp_dir2 / fname

    with pytest.raises(NotImplementedError) as exc_info:
        save_data(df, p_obj, "feather")

    assert '"feather" no longer supported.' in exc_info.value.args[0]


def test_driver_feather_read_backwards_compat(tmp_dir2):
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3]})

    fname = "some_df"
    full_file = f"{fname}.feather"

    df.to_feather(tmp_dir2 / full_file)

    obj = load_data(
        MetaRaw(full_file, "feather", "my_pin"), fsspec.filesystem("file"), tmp_dir2
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


def test_driver_apply_suffix_false(tmp_dir2):
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3]})

    fname = "some_df"
    type_ = "csv"

    p_obj = tmp_dir2 / fname
    res_fname = save_data(df, p_obj, type_, apply_suffix=False)

    assert Path(res_fname).name == "some_df"
