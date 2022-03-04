import tempfile

from io import IOBase

from typing import Protocol, Sequence, Optional, Mapping

from .versions import VersionRaw, guess_version
from .meta import Meta, MetaFactory
from .errors import PinsError
from .drivers import load_data, save_data, default_title


class IFileSystem(Protocol):
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

    def mkdirs(self, path, create_parents=True, **kwargs) -> None:
        ...

    def rm(self, path, recursive=False, maxdepth=None) -> None:
        ...


class BaseBoard:
    def __init__(
        self, board: str, fs: IFileSystem, meta_factory=MetaFactory(),
    ):
        self.board = board
        self.fs = fs
        self.meta_factory = meta_factory

    def pin_exists(self, name: str) -> bool:
        """Determine if a pin exists.

        Parameters
        ----------
        name : str
        """

        return self.fs.exists(self.path_to_pin(name))

    def pin_versions(self, name: str, as_df: bool = True) -> Sequence[VersionRaw]:
        if not self.pin_exists(name):
            raise PinsError("Cannot check version, since pin %s does not exist" % name)

        versions_raw = self.fs.ls(self.path_to_pin(name))

        # get a list of Version(Raw) objects
        all_versions = []
        for full_path in versions_raw:
            version = self.keep_final_path_component(full_path)
            all_versions.append(guess_version(version))

        # sort them, with latest last
        sorted_versions = self.sort_pin_versions(all_versions)

        # TODO(defer): this deviates from R pins, which returns a df by default
        if as_df:
            import pandas as pd

            return pd.DataFrame([v.to_dict() for v in sorted_versions])

        return sorted_versions

    def pin_meta(self, name, version: str = None):

        # determine pin version ----
        if version is not None:
            # ensure pin and version exist
            if not self.fs.exists(self.construct_path([self.board, name, version])):
                raise PinsError(
                    f"Pin {name} either does not exist, "
                    f"or is missing version: {version}."
                )

            selected_version = version
        else:
            # otherwise, get the last pin version
            versions = self.pin_versions(name, as_df=False)

            if not len(versions):
                raise NotImplementedError("TODO: sanity check when no versions")

            # select last version ----
            selected_version = versions[-1].version

        components = [self.board, name, selected_version]
        meta_name = self.meta_factory.get_meta_name(*components)

        path_version = self.construct_path([*components, meta_name])
        f = self.fs.open(path_version)
        return self.meta_factory.read_yaml(f)

    def pin_list(self):
        full_paths = self.fs.ls(self.board)
        return list(map(self.keep_final_path_component, full_paths))

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
        meta = self.pin_fetch(name, version)

        if hash is not None:
            raise NotImplementedError("TODO: validate hash")

        return load_data(
            meta, self.fs, self.construct_path([self.board, name, meta.version.version])
        )

    def pin_write(
        self,
        x,
        name: Optional[str] = None,
        type: Optional[str] = None,
        title: Optional[str] = None,
        description: Optional[str] = None,
        metadata: Optional[Mapping] = None,
        versioned: Optional[bool] = None,
    ):
        if name is None:
            raise NotImplementedError("Name must be specified.")

        if versioned is False:
            raise NotImplementedError("Only writing versioned pins supported.")

        if type is None:
            raise NotImplementedError("Type argument is required.")

        if title is None:
            title = default_title(x, type)

        # write object to disk
        with tempfile.NamedTemporaryFile() as tmp_file:
            fname = tmp_file.name

            # file is saved locally in order to hash
            save_data(x, fname, type)

            meta = self.meta_factory.create(
                fname,
                type,
                title=title,
                description=description,
                user=metadata,
                name=name,
            )

            meta_name = self.meta_factory.get_meta_name()
            dst_dir_path = self.construct_path([self.board, name, meta.version.version])
            dst_meta_path = self.construct_path([dst_dir_path, meta_name])
            dst_obj_path = self.construct_path([dst_dir_path, meta.file])

            # move pin to destination ----
            # create pin version folder
            self.fs.mkdirs(dst_dir_path)

            # write metadata yaml and object files
            with self.fs.open(dst_meta_path, "w") as f:
                meta.to_yaml(f)

            self.fs.put(fname, dst_obj_path)

        return meta

    def validate_pin_name(self, name: str) -> None:
        if "/" in name:
            raise ValueError(f"Invalid pin name: {name}")

    def path_to_pin(self, name: str, safe=True) -> str:
        self.validate_pin_name(name)

        return self.construct_path([self.board, name])

    def construct_path(self, elements) -> str:
        # TODO: should be the job of IFileSystem?
        return "/".join(elements)

    def keep_final_path_component(self, path):
        return path.split("/")[-1]

    def sort_pin_versions(self, versions):
        # assume filesystem returned them with most recent last
        return versions


class BoardRsConnect(BaseBoard):
    # TODO: high-level design considerations
    #  * Able to round trip catalogue exploration -> data fetching
    #    (e.g. pin_list -> pin_read, pin_versions -> pin_read)
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # remove board string, since it's used internally, but this board
        # always connects to a specific server via fs
        self.board = ""

    # defaults work ----

    # def pin_meta(self):
    # def pin_versions(self):
    # def pin_fetch(self):
    # def pin_exists(self):

    def pin_list(self):
        # lists all pin content on RStudio Connect server
        # we can't use fs.ls, because it will list *all content*
        paged_res = self.fs.api.misc_get_applications("content_type:pin")
        results = paged_res.results

        names = [f"{cont['owner_username']}/{cont['name']}" for cont in results]
        return names

    # def pin_read(self):
    #    raise NotImplementedError()

    def pin_write(self, *args, **kwargs):
        raise NotImplementedError()

    def validate_pin_name(self, name) -> None:
        if name.count("/") > 1:
            raise ValueError(f"Invalid pin name: {name}")

    def sort_pin_versions(self, versions) -> Sequence[VersionRaw]:
        # TODO: could alternatively implement a "select_pin_version" method
        # to be used by pin_meta
        return sorted(versions, key=lambda v: int(v.version))
