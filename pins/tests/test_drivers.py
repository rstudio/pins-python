import fsspec
import pytest

from pathlib import Path

from pins.tests.helpers import rm_env

from pins.meta import MetaRaw
from pins.config import PINS_ENV_INSECURE_READ
from pins.drivers import load_data, save_data
from pins.errors import PinsInsecureReadError


@pytest.fixture
def some_joblib(tmp_dir2):
    import joblib

    p_obj = tmp_dir2 / "some.joblib"
    joblib.dump({"a": 1}, p_obj)

    return p_obj


def test_driver_roundtrip_csv(tmp_dir2):
    # TODO: I think this test highlights the challenge of getting the flow
    # between metadata, drivers, and the metafactory right.
    # There is the name of the data (relative to the pin directory), and the full
    # name of data in its temporary directory.
    import pandas as pd

    df = pd.DataFrame({"x": [1, 2, 3]})

    fname = "some_df"
    type_ = "csv"

    p_obj = tmp_dir2 / fname
    res_fname = save_data(df, p_obj, type_)

    assert Path(res_fname).name == f"{fname}.csv"

    meta = MetaRaw(f"{fname}.csv", type_, "my_pin")
    obj = load_data(meta, fsspec.filesystem("file"), tmp_dir2)

    assert df.equals(obj)


@pytest.mark.skip("TODO: complete once driver story is fleshed out")
def test_driver_roundtrip_joblib(tmp_dir2):
    pass


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
