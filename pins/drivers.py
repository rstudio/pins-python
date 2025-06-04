from collections.abc import Sequence
from pathlib import Path
from typing import Any

from pins._adaptors import Adaptor, create_adaptor

from .config import PINS_ENV_INSECURE_READ, get_allow_pickle_read
from .errors import PinsInsecureReadError
from .meta import Meta

# TODO: move IFileSystem out of boards, to fix circular import
# from .boards import IFileSystem


UNSAFE_TYPES = frozenset(["joblib"])
REQUIRES_SINGLE_FILE = frozenset(["csv", "joblib"])


def load_path(filename: str, path_to_version, pin_type=None):
    # file path creation ------------------------------------------------------
    if pin_type == "table":
        # this type contains an rds and csv files named data.{ext}, so we match
        # R pins behavior and hardcode the name
        filename = "data.csv"

    if path_to_version is not None:
        if isinstance(path_to_version, str):
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

        elif meta.type == "geoparquet":
            try:
                import geopandas as gpd
            except ModuleNotFoundError:
                raise ModuleNotFoundError(
                    'The "geopandas" package is required to read "geoparquet" type files.'
                ) from None

            return gpd.read_parquet(f)

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
    obj: "Adaptor | Any", fname, pin_type=None, apply_suffix: bool = True
) -> "str | Sequence[str]":
    # TODO: extensible saving with deferred importing
    # TODO: how to encode arguments to saving / loading drivers?
    #       e.g. pandas index options
    # TODO: would be useful to have singledispatch func for a "default saver"
    #       as argument to board, and then type dispatchers for explicit cases
    #       of saving / loading objects different ways.

    if isinstance(obj, Adaptor):
        adaptor, obj = obj, obj._d
    else:
        adaptor = create_adaptor(obj)

    if apply_suffix:
        if pin_type == "file":
            suffix = "".join(Path(obj).suffixes)
        elif pin_type == "geoparquet":
            suffix = ".parquet"
        else:
            suffix = f".{pin_type}"
    else:
        suffix = ""

    if isinstance(fname, list):
        final_name = fname
    else:
        final_name = f"{fname}{suffix}"

    if pin_type == "csv":
        adaptor.write_csv(final_name)
    elif pin_type == "arrow":
        # NOTE: R pins accepts the type arrow, and saves it as feather.
        #       we allow reading this type, but raise an error for writing.
        adaptor.write_feather(final_name)
    elif pin_type == "feather":
        msg = (
            'Saving data as type "feather" no longer supported. Use type "arrow" instead.'
        )
        raise NotImplementedError(msg)
    elif pin_type == "parquet":
        adaptor.write_parquet(final_name)
    elif pin_type == "geoparquet":
        adaptor.write_parquet(final_name)
    elif pin_type == "joblib":
        adaptor.write_joblib(final_name)
    elif pin_type == "json":
        adaptor.write_json(final_name)
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


def default_title(obj: Any, name: str) -> str:
    # Kept for backward compatibility only.
    return create_adaptor(obj).default_title(name)
