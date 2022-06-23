from pathlib import Path

from .config import get_allow_pickle_read, PINS_ENV_INSECURE_READ
from .meta import Meta
from .errors import PinsInsecureReadError

from typing import Sequence

# TODO: move IFileSystem out of boards, to fix circular import
# from .boards import IFileSystem


UNSAFE_TYPES = frozenset(["joblib"])
REQUIRES_SINGLE_FILE = frozenset(["csv", "joblib", "file"])


def _assert_is_pandas_df(x):
    import pandas as pd

    if not isinstance(x, pd.DataFrame):
        raise NotImplementedError(
            "Currently only pandas.DataFrame can be saved to a CSV."
        )


def load_data(
    meta: Meta,
    fs,
    path_to_version: "str | None" = None,
    allow_pickle_read: "bool | None" = None,
):
    """Return loaded data, based on meta type.
    Parameters
    ----------
    meta: Meta
        Information about the stored data (e.g. its type).
    fs: IFileSystem
        An abstract filesystem with a method to .open() files.
    path_to_version:
        A filepath used as the parent directory the data to-be-loaded lives in.
    """
    # TODO: extandable loading with deferred importing
    if meta.type in UNSAFE_TYPES and not get_allow_pickle_read(allow_pickle_read):
        raise PinsInsecureReadError(
            f"Reading pin type {meta.type} involves reading a pickle file, so is NOT secure."
            f"Set the allow_pickle_read=True when creating the board, or the "
            f"{PINS_ENV_INSECURE_READ}=1 environment variable.\n"
            "See:\n"
            "  * https://docs.python.org/3/library/pickle.html \n"
            "  * https://scikit-learn.org/stable/modules/model_persistence.html#security-maintainability-limitations"
        )

    # Check that only a single file name was given
    fnames = [meta.file] if isinstance(meta.file, str) else meta.file
    if len(fnames) > 1 and type in REQUIRES_SINGLE_FILE:
        raise ValueError("Cannot load data when more than 1 file")

    # file path creation ------------------------------------------------------

    if type == "table":
        # this type contains an rds and csv files named data.{ext}, so we match
        # R pins behavior and hardcode the name
        target_fname = "data.csv"
    else:
        target_fname = fnames[0]

    if path_to_version is not None:
        path_to_file = f"{path_to_version}/{target_fname}"
    else:
        path_to_file = target_fname

    # type handling -----------------------------------------------------------

    if meta.type == "csv":
        import pandas as pd

        return pd.read_csv(fs.open(path_to_file))

    elif meta.type == "arrow":
        import pandas as pd

        return pd.read_feather(fs.open(path_to_file))

    elif meta.type == "feather":
        import pandas as pd

        return pd.read_feather(fs.open(path_to_file))

    elif meta.type == "parquet":
        import pandas as pd

        return pd.read_parquet(fs.open(path_to_file))

    elif meta.type == "table":
        import pandas as pd

        return pd.read_csv(fs.open(path_to_file))

    elif meta.type == "joblib":
        import joblib

        return joblib.load(fs.open(path_to_file))

    elif meta.type == "file":
        # TODO: update to handle multiple files
        return [str(Path(fs.open(path_to_file).name).absolute())]

    raise NotImplementedError(f"No driver for type {meta.type}")


def save_data(
    obj, fname, type=None, apply_suffix: bool = True
) -> "str | Sequence[str]":
    # TODO: extensible saving with deferred importing
    # TODO: how to encode arguments to saving / loading drivers?
    #       e.g. pandas index options
    # TODO: would be useful to have singledispatch func for a "default saver"
    #       as argument to board, and then type dispatchers for explicit cases
    #       of saving / loading objects different ways.

    if apply_suffix:
        final_name = f"{fname}.{type}"
    else:
        final_name = fname

    if type == "csv":
        _assert_is_pandas_df(obj)

        obj.to_csv(final_name, index=False)

    elif type == "arrow":
        # NOTE: R pins accepts the type arrow, and saves it as feather.
        #       we allow reading this type, but raise an error for writing.
        _assert_is_pandas_df(obj)

        obj.to_feather(final_name)

    elif type == "feather":
        _assert_is_pandas_df(obj)

        raise NotImplementedError(
            'Saving data as type "feather" no longer supported. Use type arrow instead.'
        )

    elif type == "parquet":
        _assert_is_pandas_df(obj)

        obj.to_parquet(final_name)

    elif type == "joblib":
        import joblib

        joblib.dump(obj, final_name)
    else:
        raise NotImplementedError(f"Cannot save type: {type}")

    return final_name


def default_title(obj, name):
    import pandas as pd

    if isinstance(obj, pd.DataFrame):
        # TODO(compat): title says CSV rather than data.frame
        # see https://github.com/machow/pins-python/issues/5
        shape_str = " x ".join(map(str, obj.shape))
        return f"{name}: a pinned {shape_str} DataFrame"
    else:
        obj_name = type(obj).__qualname__
        return f"{name}: a pinned {obj_name} object"
