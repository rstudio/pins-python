import builtins

from pathlib import Path

from .meta import Meta


# TODO: move IFileSystem out of boards, to fix circular import
# from .boards import IFileSystem


def load_data(meta: Meta, fs, path_to_version: "str | None" = None):
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

    # Check that only a single file name was given
    fnames = [meta.file] if isinstance(meta.file, str) else meta.file
    if len(fnames) > 1:
        raise ValueError("Cannot load data when more than 1 file")

    # TODO: currently only can load a single file
    target_fname = fnames[0]
    if path_to_version is not None:
        path_to_file = f"{path_to_version}/{target_fname}"
    else:
        path_to_file = target_fname

    if meta.type == "csv":
        import pandas as pd

        return pd.read_csv(fs.open(path_to_file), index_col=0)

    elif meta.type == "joblib":
        import joblib

        return joblib.load(fs.open(path_to_file))

    elif meta.type == "file":
        # TODO: update to handle multiple files
        return [str(Path(fs.open(path_to_file).name).absolute())]

    raise NotImplementedError(f"No driver for type {meta.type}")


def save_data(obj, fname, type=None):
    # TODO: extensible saving with deferred importing
    # TODO: how to encode arguments to saving / loading drivers?
    #       e.g. pandas index options
    # TODO: would be useful to have singledispatch func for a "default saver"
    #       as argument to board, and then type dispatchers for explicit cases
    #       of saving / loading objects different ways.
    if type == "csv":
        import pandas as pd

        if not isinstance(obj, pd.DataFrame):
            raise NotImplementedError(
                "Currently only pandas.DataFrame can be saved to a CSV."
            )
        obj.to_csv(fname)
    elif type == "joblib":
        import joblib

        joblib.dump(obj, fname)
    else:
        raise NotImplementedError(f"Cannot save type: {type}")


def default_title(obj, type):
    if type == "csv":
        import pandas as pd

        if isinstance(obj, pd.DataFrame):
            # TODO(compat): title says CSV rather than data.frame
            # see https://github.com/machow/pins-python/issues/5
            shape_str = " x ".join(map(str, obj.shape))
            return f"A pinned {shape_str} CSV"
        raise NotImplementedError(
            f"No default csv title support for class: {builtins.type(obj)}"
        )

    raise NotImplementedError(f"Cannot create default title for type: {type}")
