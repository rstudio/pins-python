import logging
import tempfile
import shutil
import inspect
import re
import functools

from io import IOBase
from pathlib import Path
from importlib_resources import files
from datetime import datetime, timedelta

from typing import Protocol, Sequence, Optional, Mapping

from .versions import VersionRaw, guess_version, version_setup
from .meta import Meta, MetaRaw, MetaFactory
from .errors import PinsError, PinsVersionError
from .drivers import load_data, save_data, load_file, default_title
from .utils import inform, warn_deprecated, ExtendMethodDoc
from .config import get_allow_rsc_short_name
from .cache import PinsCache


_log = logging.getLogger(__name__)


class IFileSystem(Protocol):

    protocol: "str | list"

    def ls(self, path: str) -> Sequence[str]:
        ...

    def put(self) -> None:
        ...

    def open(self, path: str, mode: str, *args, **kwargs) -> IOBase:
        ...

    def get(self) -> None:
        ...

    def exists(self, path: str, **kwargs) -> bool:
        ...

    def mkdir(self, path, create_parents=True, **kwargs) -> None:
        ...

    def rm(self, path, recursive=False, maxdepth=None) -> None:
        ...

    def info(self, path):
        ...


class BaseBoard:
    reserved_pin_names = {"_pins.yaml"}

    def __init__(
        self,
        board: "str | Path",
        fs: IFileSystem,
        versioned=True,
        meta_factory=MetaFactory(),
        allow_pickle_read: "bool | None" = None,
    ):
        self.board = str(board)
        self.fs = fs
        self.meta_factory = meta_factory
        self.versioned = versioned
        self.allow_pickle_read = allow_pickle_read

    def pin_exists(self, name: str) -> bool:
        """Determine if a pin exists.

        Parameters
        ----------
        name : str
            Pin name.
        """

        return self.fs.exists(self.construct_path([self.path_to_pin(name)]))

    def pin_versions(self, name: str, as_df: bool = True) -> Sequence[VersionRaw]:
        """Return available versions of a pin.

        Parameters
        ----------
        name:
            Pin name.

        """

        if not self.pin_exists(name):
            raise PinsError("Cannot check version, since pin %s does not exist" % name)

        detail = isinstance(self, BoardRsConnect)

        versions_raw = self.fs.ls(
            self.construct_path([self.path_to_pin(name)]), detail=detail
        )

        # get a list of Version(Raw) objects
        all_versions = []
        for full_path in versions_raw:
            version = self.keep_final_path_component(full_path)
            all_versions.append(guess_version(version))

        # sort them, with latest last
        sorted_versions = self.sort_pin_versions(all_versions)

        if as_df:
            import pandas as pd

            return pd.DataFrame([v.to_dict() for v in sorted_versions])

        return sorted_versions

    def pin_meta(self, name, version: str = None) -> Meta:
        """Return metadata about a pin.

        Parameters
        ----------
        name:
            Pin name.
        version: optional
            A specific pin version to retrieve.

        See Also
        --------
        [](`~pins.boards.BaseBoard.pin_versions`)

        """

        pin_name = self.path_to_pin(name)

        # determine pin version -----------------------------------------------

        if version is not None:
            # ensure pin and version exist
            if not self.fs.exists(self.construct_path([pin_name, version])):
                raise PinsError(
                    f"Pin {name} either does not exist, "
                    f"or is missing version: {version}."
                )

            selected_version = guess_version(version)
        else:
            # otherwise, get the last pin version
            versions = self.pin_versions(name, as_df=False)

            if not len(versions):
                raise NotImplementedError("TODO: sanity check when no versions")

            # select last version ----
            selected_version = versions[-1]

        # fetch metadata for version ------------------------------------------

        components = [pin_name, selected_version.version]
        meta_name = self.meta_factory.get_meta_name(*components)

        path_meta = self.construct_path([*components, meta_name])
        f, local = self._open_pin_meta(path_meta)

        meta = self.meta_factory.read_pin_yaml(
            f, pin_name, selected_version, local=local
        )

        return meta

    def pin_list(self):
        """List names of all pins in a board.

        Notes
        -----
        This is a low-level function; use [](`~pins.boards.BaseBoard.pin_search`) to get more data about
        each pin in a convenient form.
        """

        full_paths = self.fs.ls(self.board, detail=False)
        pin_names = map(self.keep_final_path_component, full_paths)

        return [name for name in pin_names if name not in self.reserved_pin_names]

    def pin_fetch(self, name: str, version: Optional[str] = None) -> Meta:
        meta = self.pin_meta(name, version)

        # TODO: sanity check caching (since R pins does a cache touch here)
        # path = self.construct_path([self.board, name, version])
        # self.fs.get(...)

        # TODO: pin_fetch R lib uses this chance to cache the files
        #       need to ensure user can have a readable cache
        #       so they could pin_fetch and then examine the result, a la pin_download
        return meta

    def pin_read(self, name, version: Optional[str] = None, hash: Optional[str] = None):
        """Return the data stored in a pin.

        Parameters
        ----------
        name:
            Pin name.
        version:
            A specific pin version to retrieve.
        hash:
            A hash used to validate the retrieved pin data. If specified, it is
            compared against the `pin_hash` field retrived by [](`~pins.boards.BaseBoard.pin_meta`).

        """
        meta = self.pin_fetch(name, version)

        if isinstance(meta, MetaRaw):
            raise TypeError(
                "Could not find metadata for this pin version. If this is an individual "
                "file, may need to use pin_download()."
            )

        if hash is not None:
            raise NotImplementedError("TODO: validate hash")

        pin_name = self.path_to_pin(name)

        return self._load_data(
            meta, self.construct_path([pin_name, meta.version.version])
        )

    def _pin_store(
        self,
        x,
        name: Optional[str] = None,
        type: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Mapping] = None,
        versioned: Optional[bool] = None,
        created: Optional[datetime] = None,
    ) -> Meta:

        if type == "feather":
            warn_deprecated(
                'Writing pin type "feather" is unsupported. Switching type to "arrow".'
                " This produces the exact same behavior, and also works with R pins."
                ' Please switch to pin_write using type="arrow".'
            )
            type = "arrow"

        if type == "file":
            # the file type makes the name of the data the exact filename, rather
            # than the pin name + a suffix (e.g. my_pin.csv).
            if isinstance(x, (tuple, list)) and len(x) == 1:
                x = x[0]

            _p = Path(x)
            _base_len = len(_p.name) - len("".join(_p.suffixes))
            object_name = _p.name[:_base_len]
        else:
            object_name = None

        pin_name = self.path_to_pin(name)

        with tempfile.TemporaryDirectory() as tmp_dir:
            # create all pin data (e.g. data.txt, save object)
            meta = self.prepare_pin_version(
                tmp_dir,
                x,
                pin_name,
                type,
                title,
                description,
                metadata,
                versioned,
                created,
                object_name=object_name,
            )

            # move pin to destination ----
            # create pin version folder
            dst_pin_path = self.construct_path([pin_name])
            dst_version = meta.version.version
            dst_version_path = self.path_to_deploy_version(name, dst_version)

            if not self.fs.exists(dst_pin_path):
                # equivalent to mkdirp, want to fail quietly in case of race conditions
                try:
                    self.fs.mkdir(dst_pin_path)
                except FileExistsError:
                    pass

            # put tmp pin dir onto backend filesystem
            # TODO: if we allow the rsc backend to fs.exists("<user>/<content>/latest")
            #       and fs.put latest, then we don't have to check the two paths differ
            if self.fs.exists(dst_version_path) and dst_version_path != dst_pin_path:
                # note that we only raise an error if version path is a subdir
                # of the pin path.
                raise PinsError(
                    f"Attempting to write pin version to {dst_version_path}, "
                    "but that directory already exists."
                )

            inform(
                _log, f"Writing pin:\nName: {repr(pin_name)}\nVersion: {dst_version}"
            )

            res = self.fs.put(tmp_dir, dst_version_path, recursive=True)

        if dst_version_path == dst_pin_path:
            # TODO(refactor): this is a RSConnect specific hack
            # since we don't know the bundle id ahead of time, the meta version
            # object is incorrect. Could fix through the meta_factory
            bundle_version = VersionRaw(res.split("/")[-1])
            meta.version = bundle_version

        return meta

    def pin_write(
        self,
        x,
        name: Optional[str] = None,
        type: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Mapping] = None,
        versioned: Optional[bool] = None,
        created: Optional[datetime] = None,
    ) -> Meta:
        """Write a pin object to the board.

        Parameters
        ----------
        x:
            An object (e.g. a pandas DataFrame) to pin.
        name:
            Pin name.
        type:
            File type used to save `x` to disk. May be "csv", "arrow", "parquet",
            "joblib", or "json".
        title:
            A title for the pin; most important for shared boards so that others
            can understand what the pin contains. If omitted, a brief description
            of the contents will be automatically generated.
        description:
            A detailed description of the pin contents.
        metadata:
            A dictionary containing additional metadata to store with the pin.
            This gets stored on the Meta.user field.
        versioned:
            Whether the pin should be versioned. Defaults to versioning.
        created:
            A date to store in the Meta.created field. This field may be used as
            part of the pin version name.
        """

        if type == "file":
            raise NotImplementedError(
                ".pin_write() does not support type='file'. "
                "Use .pin_upload() to save a file as a pin."
            )

        return self._pin_store(
            x, name, type, title, description, metadata, versioned, created
        )

    def pin_download(self, name, version=None, hash=None) -> Sequence[str]:
        """Download the files contained in a pin.

        This method only downloads the files in a pin. In order to read and load
        pin data as an object (e.g. a pandas DataFrame), use [](`~pins.boards.BaseBoard.pin_read`).

        Parameters
        ----------
        name:
            Pin name.
        version:
            A specific pin version to retrieve.
        hash:
            A hash used to validate the retrieved pin data. If specified, it is
            compared against the `pin_hash` field retrived by [](`~pins.boards.BaseBoard.pin_meta`).

        """

        meta = self.pin_fetch(name, version)

        if hash is not None:
            raise NotImplementedError("TODO: validate hash")

        pin_name = self.path_to_pin(name)

        # TODO: raise for multiple files
        # fetch file
        with load_file(
            meta, self.fs, self.construct_path([pin_name, meta.version.version])
        ) as f:
            # could also check whether f isinstance of PinCache
            fname = getattr(f, "name", None)

            if fname is None:
                raise PinsError("pin_download requires a cache.")

            return [str(Path(fname).absolute())]

    def pin_upload(
        self,
        paths: "str | list[str]",
        name=None,
        title=None,
        description=None,
        metadata=None,
    ):
        """Write a pin based on paths to one or more files.

        This method simply uploads the files given, so they can be downloaded later
        using [](`~pins.boards.BaseBoard.pin_download`).

        Parameters
        ----------
        paths:
            Paths of files to upload. Currently, only uploading a single file
            is supported.
        name:
            Pin name.
        title:
            A title for the pin; most important for shared boards so that others
            can understand what the pin contains. If omitted, a brief description
            of the contents will be automatically generated.
        description:
            A detailed description of the pin contents.
        metadata:
            A dictionary containing additional metadata to store with the pin.
            This gets stored on the Meta.user field.
        """

        return self._pin_store(
            paths,
            name,
            type="file",
            title=title,
            description=description,
            metadata=metadata,
        )

    def pin_version_delete(self, name: str, version: str):
        """Delete a single version of a pin.

        Parameters
        ----------
        name:
            Pin name.
        version:
            Version identifier.
        """

        pin_name = self.path_to_pin(name)

        pin_version_path = self.construct_path([pin_name, version])
        self.fs.rm(pin_version_path, recursive=True)

    def pin_versions_prune(
        self, name, n: "int | None" = None, days: "int | None" = None
    ):
        """Delete old versions of a pin.

        Parameters
        ----------
        name:
            Pin name.
        n, days:
            Pick one of `n` or `days` to choose how many versions to keep. `n = 3` will
            keep the last three versions; `days = 14` will keep all the versions in
            the last 14 days.

        Notes
        -----
        Regardless of what values you set, `pin_versions_prune` will never delete
        the most recent version.

        """

        if n is None and days is None:
            raise ValueError("Cannot specify both n and days.")

        versions = self.pin_versions(name, as_df=False)
        if n is not None:
            if n <= 0:
                raise ValueError("Argument n is {n}, but must be greater than 0.")

            to_delete = versions[:-n]
        if days is not None:
            if days <= 0:
                raise ValueError("Argument days is {days}, but must be greater than 0.")

            date_cutoff = datetime.today() - timedelta(days=days)
            to_delete = [v for v in versions if v.created < date_cutoff]

        # message user about deletions ----
        # TODO(question): how to pin_inform? Log or warning?
        if to_delete:
            str_vers = ", ".join([v.version for v in to_delete])
            inform(_log, f"Deleting versions: {str_vers}.")
        if not to_delete:
            inform(_log, "No old versions to delete")

        for version in to_delete:
            self.pin_version_delete(name, version.version)

    def pin_search(self, search=None, as_df=True):
        """Search for pins.

        The underlying search method depends on the board implementation, but most
        will search for text in the pin name and title.

        Parameters
        ----------
        search:
            A string to search for. By default returns all pins.
        as_df:
            Whether to return a pandas DataFrame.

        """

        # fetch metadata ----

        names = self.pin_list()

        metas = list(map(self.pin_meta, names))

        # search pins ----

        if search:
            regex = re.compile(search) if isinstance(search, str) else search

            res = []
            for meta in metas:
                if re.search(regex, meta.name) or re.search(regex, meta.title):
                    res.append(meta)
        else:
            res = metas

        # extract specific fields out ----

        if as_df:
            # optionally pull out selected fields into a DataFrame
            import pandas as pd

            # TODO(question): was the pulling of specific fields out a v0 thing?
            extracted = list(map(self._extract_search_meta, res))
            return pd.DataFrame(extracted)

        # TODO(compat): double check on the as_df=True convention
        # TODO(compat): double check how people feel the dataframe display
        #               looks with meta objects in it.
        return res

    def pin_delete(self, names: "str | Sequence[str]"):
        """Delete a pin (or pins), removing it from the board.

        Parameters
        ----------
        names:
            The names of one or more pins to delete.
        """

        if isinstance(names, str):
            names = [names]

        for name in names:
            if not self.pin_exists(name):
                raise PinsError("Cannot delete pin, since pin %s does not exist" % name)

            path_to_pin = self.construct_path([self.path_to_pin(name)])
            self.fs.rm(path_to_pin, recursive=True)

    def pin_browse(self, name, version=None, local=False):
        """TODO: Navigate to the home of a pin, either on the internet or locally.

        Parameters
        ----------
        name:
            Pin name.
        version:
            A specific pin version to retrieve.
        local:
            Whether to open the local copy of the pin. Defaults to showing you
            the home of the pin on the internet.

        See Also
        --------
        BaseBoard.pin_versions

        """

        raise NotImplementedError()

    # pin name internal methods -----------------------------------------------
    # these methods are responsible for validating pin names, and converting
    # names to their ultimate path on the file system.

    def validate_pin_name(self, name: str) -> None:
        """Raise an error if a pin name is not valid."""

        if name and "/" in name:
            raise ValueError(f"Invalid pin name: {name}")
        elif name in self.reserved_pin_names:
            raise ValueError(f"The pin name '{name}' is reserved for internal use.")

    def path_to_pin(self, name: str) -> str:
        self.validate_pin_name(name)

        return name

    def path_to_deploy_version(self, name: str, version: str):
        return self.construct_path([self.path_to_pin(name), version])

    def construct_path(self, elements) -> str:
        # TODO: should be the job of IFileSystem?
        return "/".join([self.board] + elements)

    def keep_final_path_component(self, path):
        return path.split("/")[-1]

    # version ordering, creation ----------------------------------------------

    def sort_pin_versions(self, versions):
        # assume filesystem returned them with most recent last
        return sorted(versions, key=lambda v: v.version)

    def prepare_pin_version(
        self,
        pin_dir_path,
        x,
        name: Optional[str] = None,
        type: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Mapping] = None,
        versioned: Optional[bool] = None,
        created: Optional[datetime] = None,
        object_name: Optional[str] = None,
    ):
        meta = self._create_meta(
            pin_dir_path,
            x,
            name,
            type,
            title,
            description,
            metadata,
            versioned,
            created,
            object_name,
        )

        # handle unversioned boards
        version_setup(self, name, meta.version, versioned)

        return meta

    def _create_meta(
        self,
        pin_dir_path,
        x,
        name: Optional[str] = None,
        type: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Mapping] = None,
        versioned: Optional[bool] = None,
        created: Optional[datetime] = None,
        object_name: Optional[str] = None,
    ):
        if name is None:
            raise NotImplementedError("Name must be specified.")

        if type is None:
            raise NotImplementedError("Type argument is required.")

        if title is None:
            title = default_title(x, name)

        # create metadata from object on disk ---------------------------------
        # save all pin data to a temporary folder (including data.txt), so we
        # can fs.put it all straight onto the backend filesystem

        if object_name is None:
            p_obj = Path(pin_dir_path) / name
        else:
            p_obj = Path(pin_dir_path) / object_name

        # file is saved locally in order to hash, calc size
        file_names = save_data(x, str(p_obj), type)

        meta = self.meta_factory.create(
            pin_dir_path,
            file_names,
            type,
            title=title,
            description=description,
            user=metadata,
            name=name,
            created=created,
        )

        # write metadata to tmp pin folder
        meta_name = self.meta_factory.get_meta_name()
        src_meta_path = Path(pin_dir_path) / meta_name
        meta.to_pin_yaml(src_meta_path.open("w"))

        return meta

    def _extract_search_meta(self, meta):
        keep_fields = ["name", "type", "title", "created", "file_size"]

        d = {k: getattr(meta, k, None) for k in keep_fields}
        d["meta"] = meta
        return d

    # data loading ------------------------------------------------------------

    def _load_data(self, meta, pin_version_path):
        """Return the data object stored by a pin (e.g. a DataFrame)."""
        return load_data(
            meta, self.fs, pin_version_path, allow_pickle_read=self.allow_pickle_read
        )

    # filesystem and cache methods --------------------------------------------

    def _open_pin_meta(self, path):
        f = self.fs.open(path)
        self._touch_cache(path)

        # optional additional data to put in Meta.local
        local = {}

        return f, local

    def _get_cache_path(self, pin_name, version=None, fname=None):
        version_part = [version] if version is not None else []
        fname_part = [fname] if fname is not None else []
        p_version = self.construct_path(
            [self.path_to_pin(pin_name), *version_part, *fname_part]
        )
        return self.fs._check_file(p_version)

    def _touch_cache(self, path):
        from pins.cache import touch_access_time

        # TODO: assumes same_name set to True. Let's require this be set to
        # instantiate a pins cache.
        if not isinstance(self.fs, PinsCache):
            return
        path_to_hashed = self.fs._check_file(path)
        return touch_access_time(path_to_hashed)


def board_deparse(board: BaseBoard):
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

    The example below deparses a board connected to Posit Connect.

    >>> from pins.constructors import board_connect
    >>> board_deparse(board_connect(server_url="http://example.com", api_key="xxx"))
    "board_connect(server_url='http://example.com')"

    Note that the deparsing a Posit Connect board does not keep the api_key,
    which is sensitive information. In this case, you can set the CONNECT_API_KEY
    environment variable to connect.

    Below is an example of representing a board connected to a local folder.

    >>> from pins.constructors import board_folder
    >>> board_deparse(board_folder("a/b/c"))
    "board_folder('a/b/c')"

    >>> board_deparse(board_folder(path="a/b/c", allow_pickle_read=True))
    "board_folder('a/b/c', allow_pickle_read=True)"
    """
    if board.allow_pickle_read is not None:
        allow_pickle = f", allow_pickle_read={repr(board.allow_pickle_read)}"
    else:
        allow_pickle = ""

    prot = board.fs.protocol

    if prot == "rsc":
        url = board.fs.api.server_url
        return f"board_connect(server_url={repr(url)}{allow_pickle})"
    elif prot in ["file", ("file", "local")]:
        return f"board_folder({repr(board.board)}{allow_pickle})"
    elif set(prot) == {"s3", "s3a"}:
        return f"board_s3({repr(board.board)}{allow_pickle})"
    elif prot == "abfs":
        return f"board_azure({repr(board.board)}{allow_pickle})"
    elif set(prot) == {"gcs", "gs"} or prot == "gs":
        return f"board_gcs({repr(board.board)}{allow_pickle})"
    elif prot == "http":
        return f"board_url({repr(board.board)}, {board.pin_paths}{allow_pickle})"
    else:
        raise NotImplementedError(
            f"board deparsing currently not supported for protocol: {prot}"
        )


class BoardManual(BaseBoard):
    """Simple board that accepts a dictionary of form `pin_name: path`.

    Examples
    --------
    >>> import fsspec
    >>> import os
    >>> fs = fsspec.filesystem("github", org = "rstudio", repo = "pins-python")
    >>> pin_paths = {"df_csv": "df_csv/20220214T163720Z-9bfad/"}
    >>> board = BoardManual("pins/tests/pins-compat", fs, pin_paths=pin_paths)

    >>> board.pin_list()
    ['df_csv']

    >>> board.pin_read("df_csv")
       x  y  z
    0  1  a  3
    1  2  b  4

    """

    # TODO(question): is this class worth it? Or should the user just use fsspec?

    def __init__(self, *args, pin_paths: dict, **kwargs):
        super().__init__(*args, **kwargs)

        self.pin_paths = pin_paths

    @ExtendMethodDoc
    def pin_list(self):
        return list(self.pin_paths)

    @ExtendMethodDoc
    def pin_versions(self, *args, **kwargs):
        raise NotImplementedError("This board does not support pin_versions.")

    @ExtendMethodDoc
    def pin_meta(self, name, version=None):
        if version is not None:
            raise NotImplementedError()

        pin_name = self.path_to_pin(name)
        meta_name = self.meta_factory.get_meta_name()

        # special case where we are using http protocol to fetch what may be
        # a file. here we need to create a stripped down form of metadata, since
        # a metadata file does not exist (and we can't pull files from a version dir).
        path_to_pin = self.construct_path([pin_name])
        if self.fs.protocol == "http" and not path_to_pin.rstrip().endswith("/"):
            # create metadata, rather than read from a file
            return self.meta_factory.create_raw(path_to_pin, type="file", name=pin_name)

        # note that pins on this board should point to versions, so we use an
        # empty string to mark version (it ultimately is ignored)
        path_meta = self.construct_path([pin_name, "", meta_name])
        f, local = self._open_pin_meta(path_meta)
        meta = self.meta_factory.read_pin_yaml(f, pin_name, VersionRaw(""), local=local)

        # TODO(#59,#83): handle caching, and then re-enable pin_read.
        # self._touch_cache(path_meta)

        return meta

    @ExtendMethodDoc
    def pin_download(self, name, version=None, hash=None) -> Sequence[str]:
        meta = self.pin_meta(name, version)

        if isinstance(meta, MetaRaw):
            f = load_file(meta, self.fs, None)
        else:
            raise NotImplementedError(
                "TODO: pin_download currently can only read a url to a single file."
            )

        # could also check whether f isinstance of PinCache
        fname = getattr(f, "name", None)

        if fname is None:
            raise PinsError("pin_download requires a cache.")

        return [str(Path(fname).absolute())]

    def construct_path(self, elements):
        # TODO: in practice every call to construct_path has the first element of
        # pin name. to make this safer, we should enforce that in its signature.
        pin_name, *others = elements
        pin_path = self.pin_paths[pin_name]

        pre_components = [] if not self.board else [self.board]

        # note that for paths where version is specified, it gets omitted,
        # since pin_path should point to a pin version
        if not pin_path.endswith("/"):
            if len(others):
                raise ValueError(
                    f"pin path {pin_path} does not end in '/' so is assumed to be a"
                    f" single file. Cannot construct a path to elements {elements}."
                )
            return "/".join(pre_components + [pin_path])

        # handle paths to pins (i.e. end with /) ----
        stripped = pin_path[:-1]

        if len(others) == 0:
            return "/".join(pre_components + [pin_path])
        elif len(others) == 1:
            version = others[0]
            return "/".join(pre_components + [pin_path])
        elif len(others) == 2:
            version, meta = others

            return "/".join(pre_components + [stripped, meta])

        raise NotImplementedError(
            f"Unable to construct path from these elements: {elements}"
        )


class BoardRsConnect(BaseBoard):
    # TODO: note that board is unused in this class (e.g. it's not in construct_path())

    # TODO: should read template dynamically, not at class def'n time
    html_assets_dir: Path = files("pins") / "rsconnect/html"
    html_template: Path = files("pins") / "rsconnect/html/index.html"

    # defaults work ----

    @ExtendMethodDoc
    def pin_list(self):
        # lists all pin content on RStudio Connect server
        # we can't use fs.ls, because it will list *all content*
        paged_res = self.fs.api.misc_get_applications("content_type:pin")
        results = paged_res.results

        names = [f"{cont['owner_username']}/{cont['name']}" for cont in results]
        return names

    @ExtendMethodDoc
    def pin_write(
        self, *args, access_type=None, versioned: Optional[bool] = None, **kwargs
    ):
        """Write a pin.

        Extends parent method in the following ways:

        * Modifies content item to include any title and description changes.
        * Adds access_type argument to specify who can see content. Defaults to "acl".
        """

        f_super = super().pin_write

        # bind the original signature to get pin name
        sig = inspect.signature(f_super)
        bind = sig.bind(*args, **kwargs)
        pin_name = self.path_to_pin(bind.arguments["name"])

        if pin_name.split("/")[0] != self.user_name and not self.fs.exists(pin_name):
            # TODO: fs.mkdir here would erroneously create content for the user calling the API
            # even if they were trying to create a content item for another user. we catch it here,
            # but should also fix things downstream.
            raise PinsError(
                f"You are connected as {self.user_name}, but you are trying to create a new piece"
                f" of content for another user ({pin_name}). They must create the content before you"
                " can write to it."
            )

        # attempt to make the least number of API calls possible
        if versioned or versioned is None and self.versioned:
            # arbitrary number greater than 1
            n_versions_before = 100
        else:
            try:
                versions_df = self.pin_versions(pin_name, as_df=True)
                versions = versions_df["version"].to_list()
                n_versions_before = len(versions)
            except PinsError:
                # pin does not exist
                n_versions_before = 0

        if versioned is None:
            versioned = True if n_versions_before > 1 else self.versioned

        if versioned is False and n_versions_before > 1:
            raise PinsVersionError(
                "Pin is versioned, but you have requested a write without versions."
                "To un-version a pin, you must delete it"
            )

        # run parent function ---
        meta = f_super(*args, **kwargs)

        # update content title to reflect what's in metadata ----
        # TODO(question): R pins updates this info before writing the pin..?
        content = self.fs.info(pin_name)
        self.fs.api.patch_content_item(
            content["guid"],
            title=meta.title,
            description=meta.description or "",
            access_type=access_type or content["access_type"],
        )

        # clean up non-active pins in the case of an unversioned board
        # a pin existed before the latest pin
        if versioned is False and n_versions_before == 1:
            _log.info(f"Replacing version '{versions}' with '{meta.version.version}'")
            self.pin_version_delete(pin_name, versions[0])

        return meta

    @ExtendMethodDoc
    def pin_search(self, search=None, as_df=True):
        from pins.rsconnect.api import RsConnectApiRequestError

        paged_res = self.fs.api.misc_get_applications("content_type:pin", search=search)
        results = paged_res.results

        res = []
        for content in results:
            pin_name = f"{content['owner_username']}/{content['name']}"
            version = str(content["bundle_id"])
            try:
                meta = self.pin_meta(pin_name, version)
                res.append(meta)

            except RsConnectApiRequestError as e:
                # handles the case where admins can search content they can't access
                # verify code is for inadequate permission to access
                if e.args[0]["code"] != 19:
                    raise e
                # TODO(compatibility): R pins errors instead, see #27
                res.append(self.meta_factory.create_raw(None, type=None, name=pin_name))

        # extract specific fields out ----

        if as_df:
            # optionally pull out selected fields into a DataFrame
            import pandas as pd

            extract = []
            for entry in res:
                extract.append(self._extract_search_meta(entry))

            return pd.DataFrame(extract)

        return res

    @ExtendMethodDoc
    def pin_version_delete(self, *args, **kwargs):
        from pins.rsconnect.api import RsConnectApiRequestError

        try:
            super().pin_version_delete(*args, **kwargs)
        except RsConnectApiRequestError as e:
            if e.args[0]["code"] != 75:
                raise e

            raise PinsError("RStudio Connect cannot delete the latest pin version.")

    @ExtendMethodDoc
    def pin_versions_prune(self, *args, **kwargs):
        sig = inspect.signature(super().pin_versions_prune)
        if sig.bind(*args, **kwargs).arguments.get("days") is not None:
            raise NotImplementedError(
                "RStudio Connect board cannot prune versions using days."
            )
        super().pin_versions_prune(*args, **kwargs)

    def _open_pin_meta(self, path):
        f = self.fs.open(path)
        self._touch_cache(path)

        # optional additional data to put in Meta.local
        user_name, content_name, bundle_id = str(path).split("/")[:3]
        user_guid = self.fs._user_name_cache[user_name]
        content_guid = self.fs._content_name_cache[(user_guid, content_name)]

        local = {
            "content_id": content_guid,
            "version": bundle_id,
            "url": f"{self.fs.api.server_url}/content/{content_guid}/",
        }

        return f, local

    def validate_pin_name(self, name) -> None:
        # this should be the default behavior, expecting a full pin name.
        # but because the tests use short names, we allow it to be disabled via config
        if not get_allow_rsc_short_name() and name.count("/") != 1:
            raise ValueError(
                f"Invalid pin name: {name}"
                "\nRStudio Connect pin names must include user name. E.g. "
                "\nsome_user/mtcars, for the user some_user."
            )

        # less strict test, that allows no slash: e.g. "mtcars"
        if name.count("/") > 1 or name.lstrip().startswith("/"):
            raise ValueError(f"Invalid pin name: {name}")

    def sort_pin_versions(self, versions) -> Sequence[VersionRaw]:
        # TODO: could alternatively implement a "select_pin_version" method
        # to be used by pin_meta
        return sorted(versions, key=lambda v: int(v.version))

    def path_to_pin(self, name: str) -> str:
        self.validate_pin_name(name)

        # pin of form "<user>/<content_name>" is fully specified
        if "/" in name:
            return name

        # otherwise, prepend username to pin
        return f"{self.user_name}/{name}"

    def construct_path(self, elements):
        # no need to prefix with board
        return "/".join(elements)

    def path_to_deploy_version(self, name: str, version: str):
        # RSConnect deploys a content bundle for a new version, so we simply need
        # to fs.put to the <user>/<content_name>.
        return self.path_to_pin(name)

    @functools.cached_property
    def user_name(self):
        return self.fs.api.get_user()["username"]

    def prepare_pin_version(self, pin_dir_path, x, name: "str | None", *args, **kwargs):
        # RSC pin names can have form <user_name>/<name>, but this will try to
        # create the object in a directory named <user_name>. So we grab just
        # the <name> part.
        short_name = name.split("/")[-1]

        # TODO(compat): py pins always uses the short name, R pins uses w/e the
        # user passed, but guessing people want the long name?
        meta = super()._create_meta(pin_dir_path, x, short_name, *args, **kwargs)
        meta.name = name

        # copy in files needed by index.html ----------------------------------
        crnt_files = set([meta.file] if isinstance(meta.file, str) else meta.file)
        to_add = [str(p) for p in self.html_assets_dir.rglob("*")]
        overlap = set(to_add) & crnt_files
        if overlap:
            raise PinsError(
                f"Generating an index.html would overwrite these files: {overlap}"
            )

        # recursively copy all assets into prepped pin version dir
        shutil.copytree(self.html_assets_dir, pin_dir_path, dirs_exist_ok=True)

        # render index.html ------------------------------------------------

        all_files = [meta.file] if isinstance(meta.file, str) else meta.file
        pin_files = ", ".join(f"""<a href="{x}">{x}</a>""" for x in all_files)

        context = {
            "date": meta.version.created.replace(microsecond=0),
            "pin_name": self.path_to_pin(name),
            "pin_files": pin_files,
            "pin_metadata": meta,
            "board_deparse": board_deparse(self),
        }

        # data preview ----

        # TODO: move out data_preview logic? Can we draw some limits here?
        #       note that the R library uses jsonlite::toJSON

        import pandas as pd
        import json

        if isinstance(x, pd.DataFrame):
            # TODO(compat) is 100 hard-coded?
            # Note that we go df -> json -> dict, to take advantage of pandas type conversions
            data = json.loads(x.head(100).to_json(orient="records"))
            columns = [
                {"name": [col], "label": [col], "align": ["left"], "type": [""]}
                for col in x
            ]

            # this reproduces R pins behavior, by omitting entries that would be null
            data_no_nulls = [
                {k: v for k, v in row.items() if v is not None} for row in data
            ]

            context["data_preview"] = json.dumps(
                {"data": data_no_nulls, "columns": columns}
            )
        else:
            # TODO(compat): set display none in index.html
            context["data_preview"] = json.dumps({})

        # do not show r code if not round-trip friendly
        if meta.type in ["joblib"]:
            context["show_r_style"] = "display:none"
        else:
            context["show_r_style"] = ""

        # render html template ----

        from jinja2 import Environment

        env = Environment()
        template = env.from_string(self.html_template.read_text())

        rendered = template.render(context)
        (Path(pin_dir_path) / "index.html").write_text(rendered)

        return meta
