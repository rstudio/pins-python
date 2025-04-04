from __future__ import annotations

import os
import tempfile
import warnings
from typing import Callable

import fsspec

from .boards import BaseBoard, BoardManual, BoardRsConnect, board_deparse
from .cache import PinsAccessTimeCache, PinsCache, PinsRscCacheMapper, prefix_cache
from .config import get_cache_dir, get_data_dir

# Kept here for backward-compatibility reasons
# Note that this is not a constructor, but a function to represent them.
board_deparse  # pyright: ignore[reportUnusedExpression]


class DEFAULT:
    pass


# Board constructors ==========================================================
# note that libraries not used by board classes above are imported within these
# functions. may be worth moving these funcs into their own module.


def board(
    protocol: str,
    path: str = "",
    versioned: bool = True,
    cache: type[DEFAULT] | None = DEFAULT,
    allow_pickle_read=None,
    storage_options: dict | None = None,
    board_factory: Callable | type[BaseBoard] | None = None,
):
    """General function for constructing a pins board.

    Note that this is a lower-level function. For most use cases, use a more specific
    function like [](`~pins.board_local`), or [](`~pins.board_s3`).

    Parameters
    ----------
    protocol: str
        File system protocol. E.g. file, s3, github, rsc (for Posit Connect).
        See `fsspec.filesystem` for more information.
    path: str
        A base path the board should use. For example, the directory the board lives in,
        or the path to its S3 bucket.
    versioned: bool
        Whether or not pins should be versioned.
    cache: optional, type[DEFAULT]
        Whether to use a cache. By default, pins attempts to select the right cache
        directory, given your filesystem. If `None` is passed, then no cache will be
        used. You can set the cache using the `PINS_CACHE_DIR` environment variable.
    allow_pickle_read: optional, bool
        Whether to allow reading pins that use the pickle protocol. Pickles are unsafe,
        and can execute arbitrary code. Only allow reading pickles if you trust the
        board to execute Python code on your computer.

        You can enable reading pickles by setting this to `True`, or by setting the
        environment variable `PINS_ALLOW_PICKLE_READ`. If both are set, this argument
        takes precedence.
    storage_options:
        Additional options passed to the underlying filesystem created by
        `fsspec.filesystem`.
    board_factory:
        An optional board class to use as the constructor.

    Notes
    -----
    Many fsspec implementations of filesystems cache the searching of files, which may
    cause you to not see pins saved by other people. Disable this on these file systems
    with `storage_options = {"listings_expiry_time": 0}` on S3, or `{"cache_timeout": 0}`
    on Google Cloud Storage.

    """

    if storage_options is None:
        storage_options = {}

    # TODO: at this point should just manually construct the rsc board directly
    # from board_rsconnect...
    if protocol == "rsc":
        # TODO: register RsConnectFs with fsspec
        from pins.rsconnect.fs import RsConnectFs

        fs = RsConnectFs(**storage_options)

    elif protocol == "dbc" :
        from pins.databricks.fs import DatabricksFs        

        fs = DatabricksFs(**storage_options)

    else:
        fs = fsspec.filesystem(protocol, **storage_options)

    # wrap fs in cache ----

    if cache is DEFAULT:
        base_cache_dir = get_cache_dir()

        # manually create a subdirectory for rsc server
        if protocol == "rsc":
            # ensures each server_url is its own cache directory
            hash_prefix = storage_options["server_url"]
            board_cache = prefix_cache(fs, hash_prefix)
            cache_dir = os.path.join(base_cache_dir, board_cache)

            fs = PinsCache(
                cache_storage=cache_dir,
                fs=fs,
                hash_prefix=hash_prefix,
                same_names=True,
                mapper=PinsRscCacheMapper,
            )
        elif protocol == "dbc":
            board_cache = prefix_cache(fs, path)      
            cache_dir = os.path.join(base_cache_dir, board_cache)    

            fs = fsspec.implementations.cached.SimpleCacheFileSystem(
                cache_storage=cache_dir, 
                fs=fs, 
                hash_prefix=path,
                same_names=True
            )                    
        else:
            # ensures each subdir path is its own cache directory
            board_cache = prefix_cache(fs, path)
            cache_dir = os.path.join(base_cache_dir, board_cache)
            
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
    if board_factory is not None:
        board = board_factory(path, fs, versioned, **pickle_kwargs)
    elif protocol == "rsc":
        board = BoardRsConnect(path, fs, versioned, **pickle_kwargs) 
    else:
        board = BaseBoard(path, fs, versioned, **pickle_kwargs)
    return board


# TODO(#31): change file boards to unversioned once implemented


def board_folder(path: str, versioned=True, allow_pickle_read=None):
    """Use a local folder as a board.

    Parameters
    ----------
    path:
        The folder that will hold the board.
    versioned:
        Whether or not pins should be versioned.
    allow_pickle_read: optional, bool
        Whether to allow reading pins that use the pickle protocol. Pickles are unsafe,
        and can execute arbitrary code. Only allow reading pickles if you trust the
        board to execute Python code on your computer.

        You can enable reading pickles by setting this to `True`, or by setting the
        environment variable `PINS_ALLOW_PICKLE_READ`. If both are set, this argument
        takes precedence.
    """

    return board("file", path, versioned, cache=None, allow_pickle_read=allow_pickle_read)


def board_temp(versioned=True, allow_pickle_read=None):
    """Use a local temporary directory as a board.

    Parameters
    ----------
    versioned:
        Whether or not pins should be versioned.
    allow_pickle_read: optional, bool
        Whether to allow reading pins that use the pickle protocol. Pickles are unsafe,
        and can execute arbitrary code. Only allow reading pickles if you trust the
        board to execute Python code on your computer.

        You can enable reading pickles by setting this to `True`, or by setting the
        environment variable `PINS_ALLOW_PICKLE_READ`. If both are set, this argument
        takes precedence.
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
    """Use a local folder as a board.

    Parameters
    ----------
    versioned:
        Whether or not pins should be versioned.
    allow_pickle_read: optional, bool
        Whether to allow reading pins that use the pickle protocol. Pickles are unsafe,
        and can execute arbitrary code. Only allow reading pickles if you trust the
        board to execute Python code on your computer.

        You can enable reading pickles by setting this to `True`, or by setting the
        environment variable `PINS_ALLOW_PICKLE_READ`. If both are set, this argument
        takes precedence.
    """
    path = get_data_dir()

    return board("file", path, versioned, cache=None, allow_pickle_read=allow_pickle_read)


def board_github(
    org,
    repo,
    path="",
    token=None,
    versioned=True,
    cache=DEFAULT,
    allow_pickle_read=None,
):
    """Create a board to read and write pins from GitHub.

    Parameters
    ----------
    org:
        Name of the GitHub org (e.g. user account).
    repo:
        Name of the repo.
    path:
        A subfolder in the GitHub repo holding the board.
    token:
        An optional GitHub token.
    cache:
        Whether to use a cache. By default, pins attempts to select the right cache
        directory, given your filesystem. If `None` is passed, then no cache will be
        used. You can set the cache using the `PINS_CACHE_DIR` environment variable.
    allow_pickle_read: optional, bool
        Whether to allow reading pins that use the pickle protocol. Pickles are unsafe,
        and can execute arbitrary code. Only allow reading pickles if you trust the
        board to execute Python code on your computer.

        You can enable reading pickles by setting this to `True`, or by setting the
        environment variable `PINS_ALLOW_PICKLE_READ`. If both are set, this argument
        takes precedence.


    Notes
    -----
    This board is read only.


    Examples
    --------

    >>> import pytest; pytest.skip()

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
        storage_options={"org": org, "repo": repo, "listings_expiry_time": 0},
    )


def board_urls(*args, **kwargs):
    """DEPRECATED: This board has been renamed to board_url."""
    from .utils import warn_deprecated

    warn_deprecated(
        "board_urls has been renamed to board_url. Please use board_url instead."
    )

    return board_url(*args, **kwargs)


def board_url(path: str, pin_paths: dict, cache=DEFAULT, allow_pickle_read=None):
    """Create a board from individual URLs.

    Parameters
    ----------
    path:
        A base URL to prefix all individual pin URLs with.
    pin_paths: Mapping
        A dictionary mapping pin name to pin URL.
    cache:
        Whether to use a cache. By default, pins attempts to select the right cache
        directory, given your filesystem. If `None` is passed, then no cache will be
        used. You can set the cache using the `PINS_CACHE_DIR` environment variable.
    allow_pickle_read: optional, bool
        Whether to allow reading pins that use the pickle protocol. Pickles are unsafe,
        and can execute arbitrary code. Only allow reading pickles if you trust the
        board to execute Python code on your computer.

        You can enable reading pickles by setting this to `True`, or by setting the
        environment variable `PINS_ALLOW_PICKLE_READ`. If both are set, this argument
        takes precedence.


    Examples
    --------

    ```python
    github_raw = "https://raw.githubusercontent.com/rstudio/pins-python/main/pins/tests/pins-compat"
    pin_paths = {
      "df_csv": "df_csv/20220214T163720Z-9bfad/",
      "df_arrow": "df_arrow/20220214T163720Z-ad0c1/",
      }
    board = board_url(github_raw, pin_paths)
    board.pin_list()
    ```
    ```
    ['df_csv', 'df_arrow']
    ```
    """

    # TODO(compat): R pins' version is named board_url (no s)
    if cache is DEFAULT:
        # copied from board(). this ensures that paths in cache have the form:
        # <full_path_hash>/<version_placeholder>/<file_name>
        cache_dir = get_cache_dir()
        sub_dir = prefix_cache("http", path)
        sub_cache = f"{cache_dir}/{sub_dir}"
        fs = PinsAccessTimeCache(
            target_protocol="http", cache_storage=sub_cache, same_names=False
        )
    else:
        raise NotImplementedError("Can't currently pass own cache object")

    return BoardManual(
        path,
        fs,
        versioned=False,
        allow_pickle_read=allow_pickle_read,
        pin_paths=pin_paths,
    )


def board_connect(
    server_url=None, versioned=True, api_key=None, cache=DEFAULT, allow_pickle_read=None
):
    """Create a board to read and write pins from a Posit Connect server.

    Parameters
    ----------
    server_url:
        URL to the Posit Connect server.
    versioned:
        Whether or not pins should be versioned.
    api_key:
        API key for server. If not specified, pins will attempt to read it from
        `CONNECT_API_KEY` environment variable.
    cache:
        Whether to use a cache. By default, pins attempts to select the right cache
        directory, given your filesystem. If `None` is passed, then no cache will be
        used. You can set the cache using the `PINS_CACHE_DIR` environment variable.
    allow_pickle_read: optional, bool
        Whether to allow reading pins that use the pickle protocol. Pickles are unsafe,
        and can execute arbitrary code. Only allow reading pickles if you trust the
        board to execute Python code on your computer.

        You can enable reading pickles by setting this to `True`, or by setting the
        environment variable `PINS_ALLOW_PICKLE_READ`. If both are set, this argument
        takes precedence.


    Examples
    --------
    Pins will automatically look for the `CONNECT_SERVER` and `CONNECT_API_KEY` environment variables:

    ```python
    # where environment vars CONNECT_SERVER and CONNECT_API_KEY are set
    board = board_connect()
    ```

    Or use the [dotenv](https://saurabh-kumar.com/python-dotenv/) package to load other environment variable names from a `.env` file:

    ```python
    import os
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())

    api_key = os.getenv("MY_API_KEY")
    server_url = os.getenv("MY_CONNECT_URL")
    board = board_connect(server_url=server_url, api_key=api_key)
    ```

    In order to read a public pin, use `board_url()` with the public pin URL:

    ```python
    # for a pin at https://connect.rstudioservices.com/content/3004/
    board = board_url(
      "https://connect.rstudioservices.com/content",
      {"my_df": "3004/"}
    )
    board.pin_read("my_df")
    ```

    See Also
    --------
    [](`~pins.board_url`) : Board for connecting to individual pins, using a URL or path.

    """

    # TODO: api_key can be passed in to underlying RscApi, equiv to R's manual mode
    # TODO: otherwise, CONNECT_API_KEY and CONNECT_SERVER env vars should also work
    if server_url is None:
        # TODO: this should be inside the api class
        server_url = os.environ.get("CONNECT_SERVER")

    kwargs = dict(server_url=server_url, api_key=api_key)
    return board("rsc", None, versioned, cache, allow_pickle_read, storage_options=kwargs)


board_rsconnect = board_connect


def board_s3(
    path, versioned=True, cache=DEFAULT, allow_pickle_read=None, **storage_options
):
    """Create a board to read and write pins from an AWS S3 bucket folder.

    Parameters
    ----------
    path:
        Path of form `<bucket_name>/<optional>/<subdirectory>`.
    versioned:
        Whether or not pins should be versioned.
    cache:
        Whether to use a cache. By default, pins attempts to select the right cache
        directory, given your filesystem. If `None` is passed, then no cache will be
        used. You can set the cache using the `PINS_CACHE_DIR` environment variable.
    allow_pickle_read: optional, bool
        Whether to allow reading pins that use the pickle protocol. Pickles are unsafe,
        and can execute arbitrary code. Only allow reading pickles if you trust the
        board to execute Python code on your computer.

        You can enable reading pickles by setting this to `True`, or by setting the
        environment variable `PINS_ALLOW_PICKLE_READ`. If both are set, this argument
        takes precedence.
    storage_options:
        Additional keyword arguments to be passed to the underlying fsspec S3FileSystem.

    Notes
    -----
    The s3 board uses the fsspec library (s3fs) to handle interacting with AWS S3.
    In order to authenticate, set the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
    and (optionally) `AWS_REGION` environment variables. If you are using an
    s3-compatible storage service that is not from AWS, you can pass in the necessary
    credentials to the `storage_options` dictionary, such as `endpoint_url`, `key`, and
    `secret`. We recommend setting these as environment variables. An example using
    Backblaze B2 would look like:

    Examples
    --------
    >>> import pins
    >>> board = pins.board_s3(
    ...     "pins-test",
    ...     endpoint_url=os.getenv("FSSPEC_S3_ENDPOINT_URL"),
    ...     key=os.getenv("FSSPEC_S3_KEY"),
    ...     secret=os.getenv("FSSPEC_S3_SECRET"),
    ... )

    See <https://github.com/fsspec/s3fs>

    """
    # Warn user about the use of non-zero listings_expiry_time
    listings_expiry_time = storage_options.get("listings_expiry_time", 0)
    if listings_expiry_time != 0:
        warning_msg = """
Non-zero `listings_expiry_time` may lead to unexpected behaviour with cache operations.
We're not discouraging you from setting it to be a non-zero value,
but we strongly recommend setting it to 0 for optimal performance.
"""
        warnings.warn(warning_msg)

    # Set options to pass in. Start with storage options provided by user.
    opts = {**storage_options}
    # Set listings_expiry_time based on what's provided by user
    # or the default value of 0.
    opts.update({"listings_expiry_time": listings_expiry_time})
    return board("s3", path, versioned, cache, allow_pickle_read, storage_options=opts)


def board_gcs(path, versioned=True, cache=DEFAULT, allow_pickle_read=None):
    """Create a board to read and write pins from a Google Cloud Storage bucket folder.

    Parameters
    ----------
    path:
        Path of form `<bucket_name>/<optional>/<subdirectory>`.
    versioned:
        Whether or not pins should be versioned.
    cache:
        Whether to use a cache. By default, pins attempts to select the right cache
        directory, given your filesystem. If `None` is passed, then no cache will be
        used. You can set the cache using the `PINS_CACHE_DIR` environment variable.
    allow_pickle_read: optional, bool
        Whether to allow reading pins that use the pickle protocol. Pickles are unsafe,
        and can execute arbitrary code. Only allow reading pickles if you trust the
        board to execute Python code on your computer.

        You can enable reading pickles by setting this to `True`, or by setting the
        environment variable `PINS_ALLOW_PICKLE_READ`. If both are set, this argument
        takes precedence.

    Notes
    -----
    The gcs board uses the fsspec library (gcsfs) to handle interacting with
    Google Cloud Storage. Currently, its default mode of authentication
    is supported.

    See <https://gcsfs.readthedocs.io/#credentials>
    """

    # GCSFS uses a different name for listings_expiry_time, and then
    # fixes it under the hood
    opts = {"cache_timeout": 0}
    return board("gcs", path, versioned, cache, allow_pickle_read, storage_options=opts)


def board_azure(path, versioned=True, cache=DEFAULT, allow_pickle_read=None):
    """Create a board to read and write pins from an Azure Datalake Filesystem folder.

    Parameters
    ----------
    path:
        Path of form `<bucket_name>/<optional>/<subdirectory>`.
    versioned:
        Whether or not pins should be versioned.
    cache:
        Whether to use a cache. By default, pins attempts to select the right cache
        directory, given your filesystem. If `None` is passed, then no cache will be
        used. You can set the cache using the `PINS_CACHE_DIR` environment variable.
    allow_pickle_read: optional, bool
        Whether to allow reading pins that use the pickle protocol. Pickles are unsafe,
        and can execute arbitrary code. Only allow reading pickles if you trust the
        board to execute Python code on your computer.

        You can enable reading pickles by setting this to `True`, or by setting the
        environment variable `PINS_ALLOW_PICKLE_READ`. If both are set, this argument
        takes precedence.

    Notes
    -----
    The azure board uses the fsspec library (adlfs) to handle interacting with
    Azure Datalake Filesystem (abfs). Currently, its default mode of authentication
    is supported.

    See <https://github.com/fsspec/adlfs>
    """

    opts = {"use_listings_cache": False}
    return board("abfs", path, versioned, cache, allow_pickle_read, storage_options=opts)

def board_databricks(folder_url, versioned=True, cache=DEFAULT, allow_pickle_read=None):
    """Create a board to read and write pins from an Databricks Volume folder.

    Parameters
    ----------
    folder_url:
        The path to the target folder inside Unity Catalog. The path must include the 
        catalog, schema, and volume names, preceded by 'Volumes/', like 
        "/Volumes/my-catalog/my-schema/my-volume".
    versioned:
        Whether or not pins should be versioned.
    cache:
        Whether to use a cache. By default, pins attempts to select the right cache
        directory, given your filesystem. If `None` is passed, then no cache will be
        used. You can set the cache using the `PINS_CACHE_DIR` environment variable.
    allow_pickle_read: optional, bool
        Whether to allow reading pins that use the pickle protocol. Pickles are unsafe,
        and can execute arbitrary code. Only allow reading pickles if you trust the
        board to execute Python code on your computer.

        You can enable reading pickles by setting this to `True`, or by setting the
        environment variable `PINS_ALLOW_PICKLE_READ`. If both are set, this argument
        takes precedence.

    Notes
    -----
    The Databricks board uses...
    """

    kwargs = dict(folder_url=folder_url)
    return board("dbc", folder_url, versioned, cache, allow_pickle_read, storage_options=kwargs)
