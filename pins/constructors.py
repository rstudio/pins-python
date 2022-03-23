import appdirs
import fsspec
import os
import tempfile

from .boards import BaseBoard, BoardRsConnect


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
