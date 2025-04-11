from collections.abc import Sequence
from pathlib import Path

from .config import PINS_ENV_INSECURE_READ, get_allow_pickle_read
from .errors import PinsInsecureReadError
from .meta import Meta

# TODO: move IFileSystem out of boards, to fix circular import
# from .boards import IFileSystem


UNSAFE_TYPES = frozenset(["joblib"])
REQUIRES_SINGLE_FILE = frozenset(["csv", "joblib"])


def _assert_is_pandas_df(x, file_type: str) -> None:
    import pandas as pd

    if not isinstance(x, pd.DataFrame):
        raise NotImplementedError(
            f"Currently only pandas.DataFrame can be saved as type {file_type!r}."
        )


def load_path(filename: str, path_to_version, pin_type=None):
    # file path creation ------------------------------------------------------
    if pin_type == "table":
        # this type contains an rds and csv files named data.{ext}, so we match
        # R pins behavior and hardcode the name
        filename = "data.csv"

    if path_to_version is not None:
        if(isinstance(path_to_version), str):
            path_to_version = path_to_version.rstrip("/")
        path_to_file = f"{path_to_version}/{filename}"
    else:
        # BoardUrl doesn't have versions, and the file is the full url
        path_to_file = filename

    return path_to_file


def load_file(filename: str, fs, path_to_version, pin_type):
    return fs.open(load_path(filename, path_to_version, pin_type))


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

    with load_file(meta.file, fs, path_to_version, meta.type) as f:
        if meta.type == "csv":
            import pandas as pd

            return pd.read_csv(f)

        elif meta.type == "arrow":
            import pandas as pd

            return pd.read_feather(f)

        elif meta.type == "feather":
            import pandas as pd

            return pd.read_feather(f)

        elif meta.type == "parquet":
            import pandas as pd

            return pd.read_parquet(f)

        elif meta.type == "table":
            import pandas as pd

            return pd.read_csv(f)

        elif meta.type == "joblib":
            import joblib

            return joblib.load(f)

        elif meta.type == "json":
            import json

            return json.load(f)

        elif meta.type == "file":
            raise NotImplementedError(
                "Methods like `.pin_read()` are not able to read 'file' type pins."
                " Use `.pin_download()` to download the file."
            )

        elif meta.type == "rds":
            try:
                import rdata  # pyright: ignore[reportMissingImports]

                return rdata.read_rds(f)
            except ModuleNotFoundError:
                raise ModuleNotFoundError(
                    "Install the 'rdata' package to attempt to convert 'rds' files into Python objects."
                )

    raise NotImplementedError(f"No driver for type {meta.type}")


def save_data(
    obj, fname, pin_type=None, apply_suffix: bool = True
) -> "str | Sequence[str]":
    # TODO: extensible saving with deferred importing
    # TODO: how to encode arguments to saving / loading drivers?
    #       e.g. pandas index options
    # TODO: would be useful to have singledispatch func for a "default saver"
    #       as argument to board, and then type dispatchers for explicit cases
    #       of saving / loading objects different ways.

    if apply_suffix:
        if pin_type == "file":
            suffix = "".join(Path(obj).suffixes)
        else:
            suffix = f".{pin_type}"
    else:
        suffix = ""

    if isinstance(fname, list):
        final_name = fname
    else:
        final_name = f"{fname}{suffix}"

    if pin_type == "csv":
        _assert_is_pandas_df(obj, file_type=type)

        obj.to_csv(final_name, index=False)

    elif pin_type == "arrow":
        # NOTE: R pins accepts the type arrow, and saves it as feather.
        #       we allow reading this type, but raise an error for writing.
        _assert_is_pandas_df(obj, file_type=type)

        obj.to_feather(final_name)

    elif pin_type == "feather":
        _assert_is_pandas_df(obj, file_type=type)

        raise NotImplementedError(
            'Saving data as type "feather" no longer supported. Use type "arrow" instead.'
        )

    elif pin_type == "parquet":
        _assert_is_pandas_df(obj, file_type=type)

        obj.to_parquet(final_name)

    elif pin_type == "joblib":
        import joblib

        joblib.dump(obj, final_name)

    elif pin_type == "json":
        import json

        json.dump(obj, open(final_name, "w"))

    elif pin_type == "file":
        import contextlib
        import shutil

        if isinstance(obj, list):
            for file, final in zip(obj, final_name):
                with contextlib.suppress(shutil.SameFileError):
                    shutil.copyfile(str(file), final)
            return obj
        # ignore the case where the source is the same as the target
        else:
            with contextlib.suppress(shutil.SameFileError):
                shutil.copyfile(str(obj), final_name)

    else:
        raise NotImplementedError(f"Cannot save type: {pin_type}")

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
