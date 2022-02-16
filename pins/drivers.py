from .meta import Meta

# TODO: move IFileSystem out of boards, to fix circular import
# from .boards import IFileSystem


def load_data(meta: Meta, fs, path_to_version):
    # TODO: extandable loading with deferred importing
    if meta.type == "csv":
        import pandas as pd

        fnames = [meta.file] if isinstance(meta.file, str) else meta.file
        if len(fnames) > 1:
            raise ValueError("Cannot load CSV when more than 1 file")

        target_fname = fnames[0]
        path_to_file = f"{path_to_version}/{target_fname}"
        return pd.read_csv(fs.open(path_to_file), index_col=0)

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
    else:
        raise NotImplementedError(f"Cannot save type: {type}")
