from importlib_resources import files as _files

sources = {
    "mtcars": _files("pins") / "data/mtcars.csv",
}


def __dir__():
    return list(sources)


def __getattr__(k):
    import pandas as pd

    f_path = sources.get("mtcars")

    return pd.read_csv(f_path)
