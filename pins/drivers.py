from .meta import Meta

# TODO: move IFileSystem out of boards, to fix circular import
# from .boards import IFileSystem


def load_data(meta: Meta, fs, path_to_version):
    if meta.type == "csv":
        import pandas as pd

        fnames = [meta.file] if isinstance(meta.file, str) else meta.file
        if len(fnames) > 1:
            raise ValueError("Cannot load CSV when more than 1 file")

        target_fname = fnames[0]
        path_to_file = f"{path_to_version}/{target_fname}"
        return pd.read_csv(fs.open(path_to_file))

    raise NotImplementedError(f"No driver for type {meta.type}")
