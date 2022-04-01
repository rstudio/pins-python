import fsspec
import os
import tempfile

from .boards import BaseBoard, BoardRsConnect, BoardManual
from .cache import PinsCache, PinsUrlCache
from .config import get_data_dir, get_cache_dir


class DEFAULT:
    pass


# Board constructors ==========================================================
# note that libraries not used by board classes above are imported within these
# functions. may be worth moving these funcs into their own module.


def board(
    protocol: str,
    path: str = "",
    versioned: bool = True,
    cache: "DEFAULT | None" = DEFAULT,
    storage_options: "dict | None" = None,
    board_factory: "callable | BaseBoard | None" = None,
):
    """
    Parameters
    ----------
    protocol:
        File system protocol. E.g. file, s3, github, rsc (for RStudio Connect).
        See fsspec.filesystem for more information.
    path:
        A base path the board should use. For example, the directory the board lives in,
        or the path to its s3 bucket.
    versioned:
        Whether or not pins should be versioned.
    cache:
        Whether to use a cache. By default, pins attempts to select the right cache
        directory, given your filesystem. If None is passed, then no cache will be
        used. You can set the cache using the PINS_CACHE_DIR environment variable.
    storage_options:
        Additional options passed to the underlying filesystem created by
        fsspec.filesystem.
    board_factory:
        An optional board class to use as the constructor.
    """

    if storage_options is None:
        storage_options = {}

    if protocol == "rsc":
        # TODO: register RsConnectFs with fsspec
        from pins.rsconnect.fs import RsConnectFs

        fs = RsConnectFs(**storage_options)

    else:
        fs = fsspec.filesystem(protocol, **storage_options)

    # wrap fs in cache ----

    if cache is DEFAULT:
        cache_dir = get_cache_dir()
        fs = PinsCache(
            cache_storage=cache_dir, fs=fs, hash_prefix=path, same_names=True
        )
    elif cache is None:
        pass
    else:
        NotImplemented("Can't currently pass own cache object.")

    # construct board ----

    # TODO: should use a registry or something
    if protocol == "rsc" and board_factory is None:
        board = BoardRsConnect(path, fs, versioned)
    elif board_factory is not None:
        board = board_factory(path, fs, versioned)
    else:
        board = BaseBoard(path, fs, versioned)
    return board


# TODO(#31): change file boards to unversioned once implemented


def board_folder(path, versioned=True):
    return board("file", path, versioned, cache=None)


def board_temp(versioned=True):
    tmp_dir = tempfile.TemporaryDirectory()

    board_obj = board("file", tmp_dir.name, versioned, cache=None)

    # TODO: this is necessary to ensure the temporary directory dir persists.
    # without maintaining a reference to it, it could be deleted after this
    # function returns
    board_obj.__tmp_dir = tmp_dir

    return board_obj


def board_local(versioned=True):
    path = get_data_dir()

    return board("file", path, versioned, cache=None)


def board_github(org, repo, path="", versioned=True, cache=DEFAULT):
    """Returns a github pin board.

    Parameters
    ----------
    path:
        TODO
    versioned:
        TODO
    org:
        Name of the github org (e.g. user account).
    repo:
        Name of the repo.


    Note
    ----
    This board is read only.


    Examples
    --------

    >>> board = board_github("machow", "pins-python", "pins/tests/pins-compat")
    >>> board.pin_list()
    ['df_arrow', 'df_csv', 'df_rds', 'df_unversioned']

    >>> board.pin_read("df_csv")
       y  z
    x
    1  a  3
    2  b  4

    """

    return board(
        "github", path, versioned, cache, storage_options={"org": org, "repo": repo}
    )


def board_urls(path: str, pin_paths: dict, cache=DEFAULT):
    """

    Example
    -------

    >>> github_raw = "https://raw.githubusercontent.com/"
    >>> pin_paths = {
    ...     "df_csv": "df_csv/20220214T163720Z-9bfad",
    ...     "df_arrow": "df_arrow/20220214T163720Z-ad0c1",
    ... }
    >>> board = board_urls(github_raw, pin_paths)
    >>> board.pin_list()
    ['df_csv', 'df_arrow']
    """

    # TODO(compat): R pins' version is named board_url (no s)
    if cache is DEFAULT:
        # copied from board(). this ensures that paths in cache have the form:
        # <full_path_hash>/<version_placeholder>/<file_name>
        cache_dir = get_cache_dir()
        fs = PinsUrlCache(
            target_protocol="http", cache_storage=cache_dir, same_names=True
        )
    else:
        raise NotImplementedError("Can't currently pass own cache object")

    return BoardManual(path, fs, versioned=True, pin_paths=pin_paths)


def board_rsconnect(versioned=True, server_url=None, api_key=None, cache=DEFAULT):
    """

    Parameters
    ----------
    server_url:
        TODO
    api_key:
        TODO
    """

    # TODO: api_key can be passed in to underlying RscApi, equiv to R's manual mode
    # TODO: otherwise, CONNECT_API_KEY and CONNECT_SERVER env vars should also work
    if server_url is None:
        # TODO: this should be inside the api class
        server_url = os.environ.get("CONNECT_SERVER")

    kwargs = dict(server_url=server_url, api_key=api_key)
    return board("rsc", "", versioned, storage_options=kwargs)


def board_s3(path, versioned=True):
    # TODO: user should be able to specify storage options here?
    return board("s3", path, versioned)
