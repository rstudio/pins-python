from io import IOBase

from typing import Protocol, Sequence
from .versions import VersionRaw, guess_version
from .meta import Meta, MetaLoader
from .errors import PinsError


class IFileSystem(Protocol):
    def ls(self) -> Sequence[str]:
        ...

    def put(self) -> None:
        ...

    def open(self) -> IOBase:
        ...

    def get(self) -> None:
        ...

    def exists(self) -> bool:
        ...


class BaseBoard:
    def __init__(
        self,
        board: str,
        fs: IFileSystem,
        meta_factory: Meta = Meta,
        meta_loader=MetaLoader(),
    ):
        self.board = board
        self.fs = fs
        self.meta_factory = meta_factory
        self.meta_loader = meta_loader

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

        # TODO(defer): this deviates from R pins, which returns a df by default
        if as_df:
            import pandas as pd

            return pd.DataFrame([v.to_dict() for v in all_versions])

        return all_versions

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
        meta_name = self.meta_loader.get_meta_name(*components)

        path_version = self.construct_path([*components, meta_name])
        f = self.fs.open(path_version)
        return self.meta_loader.load(f)

    def pin_list(self):
        full_paths = self.fs.ls(self.board)
        return list(map(self.keep_final_path_component, full_paths))

    def pin_write(
        self,
        x,
        name=None,
        type=None,
        title=None,
        description=None,
        metadata=None,
        versioned=None,
    ):
        pass
        # write object to disk

        # hash it

        # calc file size

        # create meta

    def validate_pin_name(self, name: str) -> bool:
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
