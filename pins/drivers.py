from pathlib import Path
from typing import Any, Sequence

from pins._adaptors import _create_adaptor

from .config import PINS_ENV_INSECURE_READ, get_allow_pickle_read
from .errors import PinsInsecureReadError
from .meta import Meta

# TODO: move IFileSystem out of boards, to fix circular import
# from .boards import IFileSystem


UNSAFE_TYPES = frozenset(["joblib"])
REQUIRES_SINGLE_FILE = frozenset(["csv", "joblib", "file"])


def load_path(meta, path_to_version):
    # Check that only a single file name was given
    fnames = [meta.file] if isinstance(meta.file, str) else meta.file
    if len(fnames) > 1 and type in REQUIRES_SINGLE_FILE:
        raise ValueError("Cannot load data when more than 1 file")

    # file path creation ------------------------------------------------------

    if type == "table":  # noqa: E721 False Positive due to bug: https://github.com/rstudio/pins-python/issues/266
        # this type contains an rds and csv files named data.{ext}, so we match
        # R pins behavior and hardcode the name
        target_fname = "data.csv"
    else:
        target_fname = fnames[0]

    if path_to_version is not None:
        path_to_file = f"{path_to_version}/{target_fname}"
    else:
        # BoardUrl doesn't have versions, and the file is the full url
        path_to_file = target_fname

    return path_to_file


def load_file(meta: Meta, fs, path_to_version):
    return fs.open(load_path(meta, path_to_version))


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

    with load_file(meta, fs, path_to_version) as f:
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
                import rdata

                return rdata.read_rds(f)
            except ModuleNotFoundError:
                raise ModuleNotFoundError(
                    "Install the 'rdata' package to attempt to convert 'rds' files into Python objects."
                )

    raise NotImplementedError(f"No driver for type {meta.type}")


def save_data(obj, fname, type=None, apply_suffix: bool = True) -> "str | Sequence[str]":
    # TODO: extensible saving with deferred importing
    # TODO: how to encode arguments to saving / loading drivers?
    #       e.g. pandas index options
    # TODO: would be useful to have singledispatch func for a "default saver"
    #       as argument to board, and then type dispatchers for explicit cases
    #       of saving / loading objects different ways.

    adaptor = _create_adaptor(obj)

    if apply_suffix:
        if type == "file":
            suffix = "".join(Path(obj).suffixes)
        else:
            suffix = f".{type}"
    else:
        suffix = ""

    final_name = f"{fname}{suffix}"

    if type == "csv":
        adaptor.write_csv(final_name)
    elif type == "arrow":
        # NOTE: R pins accepts the type arrow, and saves it as feather.
        #       we allow reading this type, but raise an error for writing.
        adaptor.write_feather(final_name)
    elif type == "feather":
        msg = (
            'Saving data as type "feather" no longer supported. Use type "arrow" instead.'
        )
        raise NotImplementedError(msg)
    elif type == "parquet":
        adaptor.write_parquet(final_name)
    elif type == "joblib":
        adaptor.write_joblib(final_name)
    elif type == "json":
        adaptor.write_json(final_name)
    elif type == "file":
        import contextlib
        import shutil

        # ignore the case where the source is the same as the target
        with contextlib.suppress(shutil.SameFileError):
            shutil.copyfile(str(obj), final_name)
    else:
        raise NotImplementedError(f"Cannot save type: {type}")

    return final_name


def default_title(obj: Any, name: str) -> str:
    # Kept for backward compatibility only.
    return _create_adaptor(obj).default_title(name)
