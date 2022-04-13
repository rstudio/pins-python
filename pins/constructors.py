import fsspec
import os
import tempfile

from .boards import BaseBoard, BoardRsConnect, BoardManual
from .cache import PinsCache, PinsUrlCache, PinsRscCache
from .config import get_data_dir, get_cache_dir


class DEFAULT:
    pass


# Representing constructors ===================================================


def deparse_board(board: BaseBoard):
    """Return a representation of how a board could be reconstructed.

    Note that this function does not try to represent the exact arguments used
    to construct a board, but key pieces (like the path to the board). You may
    need to specify environment variables with API keys to complete the connection.

    Parameters
    ----------
    board:
        A pins board to be represented.

    Examples
    --------

    The example below deparses a board connected to RStudio Connect.

    >>> deparse_board(board_rsconnect(server_url="http://example.com", api_key="xxx"))
    "board_rsconnect(server_url='http://example.com')"

    Note that the deparsing an RStudio Connect board does not keep the api_key,
    which is sensitive information. In this case, you can set the CONNECT_API_KEY
    environment variable to connect.

    Below is an example of representing a board connected to a local folder.

    >>> deparse_board(board_folder("a/b/c"))
    "board_folder('a/b/c')"
    """

    prot = board.fs.protocol
    if prot == "rsc":
        url = board.fs.api.server_url
        return f"board_rsconnect(server_url={repr(url)})"
    elif prot == "file":
        return f"board_folder({repr(board.board)})"
    else:
        raise NotImplementedError(
            "board deparsing currently not supported for protocol: {prot}"
        )


# Board constructors ==========================================================
# note that libraries not used by board classes above are imported within these
# functions. may be worth moving these funcs into their own module.


def board(
    protocol: str,
    path: str = "",
    versioned: bool = True,
    cache: "DEFAULT | None" = DEFAULT,
    allow_pickle_read=None,
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
    allow_pickle_read: optional, bool
        Whether to allow reading pins that use the pickle protocol. Pickles are unsafe,
        and can execute arbitrary code. Only allow reading pickles if you trust the
        board to be able to execute python code on your computer.

        You can enable reading pickles by setting this to True, or by setting the
        environment variable PINS_ALLOW_PICKLE_READ. If both are set, this argument
        takes precedence.
    storage_options:
        Additional options passed to the underlying filesystem created by
        fsspec.filesystem.
    board_factory:
        An optional board class to use as the constructor.
    """

    if storage_options is None:
        storage_options = {}

    # TODO: at this point should just manually construct the rsc board directly
    # from board_rsconnect...
    if protocol == "rsc":
        # TODO: register RsConnectFs with fsspec
        from pins.rsconnect.fs import RsConnectFs

        fs = RsConnectFs(**storage_options)

    else:
        fs = fsspec.filesystem(protocol, **storage_options)

    # wrap fs in cache ----

    if cache is DEFAULT:
        cache_dir = get_cache_dir()

        # manually create a subdirectory for rsc server
        if protocol == "rsc":
            hash_prefix = storage_options["server_url"]
            fs = PinsRscCache(
                cache_storage=cache_dir, fs=fs, hash_prefix=hash_prefix, same_names=True
            )
        else:
            fs = PinsCache(
                cache_storage=cache_dir, fs=fs, hash_prefix=path, same_names=True
            )
    elif cache is None:
        pass
    else:
        NotImplemented("Can't currently pass own cache object.")

    # construct board ----

    pickle_kwargs = {"allow_pickle_read": allow_pickle_read}
    # TODO: should use a registry or something
    if protocol == "rsc" and board_factory is None:
        board = BoardRsConnect(path, fs, versioned, **pickle_kwargs)
    elif board_factory is not None:
        board = board_factory(path, fs, versioned, **pickle_kwargs)
    else:
        board = BaseBoard(path, fs, versioned, **pickle_kwargs)
    return board


# TODO(#31): change file boards to unversioned once implemented


def board_folder(path: str, versioned=True, allow_pickle_read=None):
    """Create a pins board inside a folder.

    Parameters
    ----------
    path:
        The folder that will hold the board.
    **kwargs:
        Passed to the pins.board function.
    """

    return board(
        "file", path, versioned, cache=None, allow_pickle_read=allow_pickle_read
    )


def board_temp(versioned=True, allow_pickle_read=None):
    """Create a pins board in a temporary directory.

    Parameters
    ----------
    **kwargs:
        Passed to the pins.board function.
    """

    tmp_dir = tempfile.TemporaryDirectory()

    board_obj = board(
        "file", tmp_dir.name, versioned, cache=None, allow_pickle_read=allow_pickle_read
    )

    # TODO: this is necessary to ensure the temporary directory dir persists.
    # without maintaining a reference to it, it could be deleted after this
    # function returns
    board_obj.__tmp_dir = tmp_dir

    return board_obj


def board_local(versioned=True, allow_pickle_read=None):
    """Create a board in a system data directory.

    Parameters
    ----------
    **kwargs:
        Passed to the pins.board function.
    """
    path = get_data_dir()

    return board(
        "file", path, versioned, cache=None, allow_pickle_read=allow_pickle_read
    )


def board_github(
    org,
    repo,
    path="",
    token=None,
    versioned=True,
    cache=DEFAULT,
    allow_pickle_read=None,
):
    """Returns a github pin board.

    Parameters
    ----------
    org:
        Name of the github org (e.g. user account).
    repo:
        Name of the repo.
    path:
        A subfolder in the github repo holding the board.
    token:
        An optional github token.
    **kwargs:
        Passed to the pins.board function.

    Note
    ----
    This board is read only.


    Examples
    --------

    >>> import os
    >>> board = board_github("machow", "pins-python", "pins/tests/pins-compat")
    >>> board.pin_list()
    ['df_arrow', 'df_csv', 'df_rds', 'df_unversioned']

    >>> board.pin_read("df_csv")
       x  y  z
    0  1  a  3
    1  2  b  4


    """

    return board(
        "github",
        path,
        versioned,
        cache,
        allow_pickle_read=allow_pickle_read,
        storage_options={"org": org, "repo": repo},
    )


def board_urls(path: str, pin_paths: dict, cache=DEFAULT, allow_pickle_read=None):
    """Create a board from individual urls.

    Parameters
    ----------
    path:
        A base url to prefix all individual pin urls with.
    pin_paths: Mapping
        A dictionary mapping pin name to pin url .
    **kwargs:
        Passed to the pins.board function.

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

    return BoardManual(
        path,
        fs,
        versioned=True,
        allow_pickle_read=allow_pickle_read,
        pin_paths=pin_paths,
    )


def board_rsconnect(
    server_url=None, versioned=True, api_key=None, cache=DEFAULT, allow_pickle_read=None
):
    """Create a board to read and write pins from an RStudio Connect instance.

    Parameters
    ----------
    server_url:
        Url to the RStudio Connect server.
    api_key:
        API key for server. If not specified, pins will attempt to read it from
        CONNECT_API_KEY environment variable.
    **kwargs:
        Passed to the pins.board function.
    """

    # TODO: api_key can be passed in to underlying RscApi, equiv to R's manual mode
    # TODO: otherwise, CONNECT_API_KEY and CONNECT_SERVER env vars should also work
    if server_url is None:
        # TODO: this should be inside the api class
        server_url = os.environ.get("CONNECT_SERVER")

    kwargs = dict(server_url=server_url, api_key=api_key)
    return board(
        "rsc", None, versioned, cache, allow_pickle_read, storage_options=kwargs
    )


def board_s3(path, versioned=True, cache=DEFAULT, allow_pickle_read=None):
    """Create a board to read and write pins from an AWS S3 bucket folder.

    Parameters
    ----------
    path:
        Path of form <bucket_name>/<optional>/<subdirectory>.
    **kwargs:
        Passed to the pins.board function.
    """
    # TODO: user should be able to specify storage options here?
    return board("s3", path, versioned, cache, allow_pickle_read)
