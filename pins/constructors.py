import appdirs
import fsspec
import os
import tempfile

from .boards import BaseBoard, BoardRsConnect, BoardManual


# Board constructors ==========================================================
# note that libraries not used by board classes above are imported within these
# functions. may be worth moving these funcs into their own module.


def board(protocol, path="", versioned=True, storage_options: "dict | None" = None):

    if storage_options is None:
        storage_options = {}

    if protocol == "rsc":
        # TODO: register RsConnectFs with fsspec
        from pins.rsconnect.fs import RsConnectFs

        fs = RsConnectFs(**storage_options)
        board = BoardRsConnect(path, fs, versioned)
    else:
        fs = fsspec.filesystem(protocol, **storage_options)
        board = BaseBoard(path, fs, versioned)

    return board


# TODO(#31): change file boards to unversioned once implemented


def board_folder(path, versioned=True):
    return board("file", path, versioned)


def board_temp(versioned=True):
    tmp_dir = tempfile.TemporaryDirectory()

    board_obj = board("file", tmp_dir.name, versioned)

    # TODO: this is necessary to ensure the temporary directory dir persists.
    # without maintaining a reference to it, it could be deleted after this
    # function returns
    board_obj.__tmp_dir = tmp_dir

    return board_obj


def board_local(versioned=True):
    path = os.environ.get("PINS_DATA_DIR", appdirs.user_data_dir("pins"))

    return board("file", path, versioned)


def board_github(path, versioned=True, org=None, repo=None):
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

    >>> board = board_github("pins/tests/pins-compat", org="machow", repo="pins-python")
    >>> board.pin_list()
    ['df_arrow', 'df_csv', 'df_rds', 'df_unversioned']

    >>> board.pin_read("df_csv")
       y  z
    x
    1  a  3
    2  b  4

    """

    return board("github", path, versioned, {"org": org, "repo": repo})


def board_urls(path: str, pin_paths: dict):
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

    # TODO(question): R pins' version is named board_url (no s)

    fs = fsspec.filesystem("http")
    return BoardManual(path, fs, pin_paths=pin_paths)


def board_rsconnect(versioned=True, server_url=None, api_key=None):
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
