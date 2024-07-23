from typing import ClassVar
from dataclasses import dataclass, asdict, field, fields, InitVar
from pathlib import Path

import yaml

from typing import Mapping, Union, Sequence, Optional, List

from .versions import VersionRaw, Version, guess_version
from ._types import StrOrFile, IOBase

META_FILENAME = "data.txt"
DEFAULT_API_VERSION = 1


@dataclass
class MetaRaw:
    """Absolute minimum metadata for a pin.

    Parameters
    ----------
    file:
        All relevant files contained in the pin. Note that these be absolute paths
        to fetch from the target filesystem.
    type:
        The type of pin data stored. This is used to determine how to read / write it.
    """

    file: "str | Sequence[str] | None"
    type: str
    name: str


@dataclass
class Meta:
    """Represent metadata for a pin version.

    Parameters
    ----------
    title:
        A title for the pin.
    description:
        A detailed description of the pin contents.
    tags:
        Optional tags applied to the pin.
    created:
        Datetime the pin was created (TODO: document format).
    pin_hash:
        A hash of the pin.
    file:
        All relevant files in the pin. Should be relative to this pin's folder.
    file_size:
        The total size of the files in the pin.
    type:
        The type of pin data stored. This is used to determine how to read / write it.
    api_version:
        The internal version of the metadata format.
    name:
        TODO - where is this in R pins?
    user:
        A dictionary of additional metadata that may be specified by the user.
    local:
        A dictionary of additional metadata that may be added by the board, depending
        on the backend used. E.g. RStudio Connect content id, url, etc..

    """

    _excluded: ClassVar["set[str]"] = {"name", "version", "local"}

    title: Optional[str]
    description: Optional[str]

    # TODO(defer): different from R pins, which has a local field
    created: str
    pin_hash: str

    file: Union[str, Sequence[str]]
    file_size: int
    type: str

    api_version: int

    # In the metadata yaml, the created field uses a custom format, so
    # we need a version object in order to render it. You can think of
    # the version here as "the thing that was used to create version_name,
    # pin_hash, created, etc.."
    version: VersionRaw

    tags: Optional[List[str]] = None
    name: Optional[str] = None
    user: Mapping = field(default_factory=dict)
    local: Mapping = field(default_factory=dict)

    unknown_fields: InitVar["dict | None"] = None

    def __post_init__(self, unknown_fields: "dict | None"):
        unknown_fields = {} if unknown_fields is None else unknown_fields

        self._unknown_fields = unknown_fields

    def __getattr__(self, k):
        try:
            return self._unknown_fields[k]
        except KeyError:
            raise AttributeError(f"No metadata field not found: {k}")

    def to_dict(self) -> Mapping:
        data = asdict(self)

        return data

    def to_pin_dict(self):
        d = self.to_dict()

        for k in self._excluded:
            del d[k]

        # TODO: once tag writing is implemented, delete this line
        del d["tags"]

        return d

    @classmethod
    def from_pin_dict(cls, data, pin_name, version, local=None) -> "Meta":
        # TODO: re-arrange Meta argument positions to reflect what's been
        # learned about default arguments. e.g. title was not used at some
        # point in api_version 1
        all_field_names = {entry.name for entry in fields(Meta)}

        keep_fields = all_field_names - cls._excluded

        extra = {"title": None} if "title" not in data else {}
        local = {} if local is None else local

        meta_data = {k: v for k, v in data.items() if k in keep_fields}
        unknown = {k: v for k, v in data.items() if k not in keep_fields}

        return cls(
            **meta_data,
            **extra,
            name=pin_name,
            version=version,
            local=local,
            unknown_fields=unknown,
        )

    def to_pin_yaml(self, f: Optional[IOBase] = None) -> "str | None":
        data = self.to_pin_dict()

        return yaml.dump(data, f)


@dataclass
class MetaV0:
    file: Union[str, Sequence[str]]
    type: str
    description: "str | None"

    name: str

    version: VersionRaw

    # holds raw data.txt contents
    original_fields: dict = field(default_factory=dict)
    user: dict = field(default_factory=dict, init=False)
    local: Mapping = field(default_factory=dict)

    title: ClassVar[None] = None
    created: ClassVar[None] = None
    pin_hash: ClassVar[None] = None
    file_size: ClassVar[None] = None
    api_version: ClassVar[None] = None

    def to_dict(self):
        return asdict(self)

    @classmethod
    def from_pin_dict(cls, data, pin_name, version, local=None) -> "MetaV0":
        # could infer from dataclasses.fields(), but seems excessive.
        req_fields = {"type", "description"}

        # Note that we need to .get(), since fields may not be in metadata
        req_inputs = {k: data.get(k) for k in req_fields}
        req_inputs["file"] = data["path"]

        local = {} if local is None else local
        return cls(
            **req_inputs,
            name=pin_name,
            original_fields=data,
            version=version,
            local=local,
        )

    def to_pin_dict(self):
        raise NotImplementedError("v0 pins metadata are read only.")

    def to_pin_yaml(self, *args, **kwargs):
        self.to_pin_dict()


class MetaFactory:
    """Responsible for creating and loading (e.g. from yaml) of meta objects."""

    def get_meta_name(self, *args, **kwargs) -> str:
        return META_FILENAME

    def get_version_for_meta(self, api_version) -> Version:
        if api_version != 1:
            raise NotImplementedError("Unsupported api_version: %s" % api_version)

        return Version

    def create(
        self,
        base_folder: "str | Path",
        files: Sequence[StrOrFile],
        type,
        # TODO: when files is a string name should be okay as None
        name,
        title,
        description=None,
        created=None,
        user=None,
    ) -> Meta:
        if title is None:
            raise NotImplementedError("title arguments required")
        if isinstance(files, str):
            from pathlib import Path

            version = Version.from_files([files], created)
            p_file = Path(files)
            file_size = p_file.stat().st_size
            file_name = str(Path(files).relative_to(Path(base_folder)))

        elif isinstance(files, IOBase):
            # TODO: in theory can calculate size from a file object, but let's
            # wait until it's clear how calculating file size fits into pins
            # e.g. in combination with folders, etc..

            # from os import fstat
            #
            # version = Version.from_files([files], created)
            # files_size = fstat(files.fileno()).st_size

            raise NotImplementedError("Cannot create from file object.")
        else:
            raise NotImplementedError("TODO: creating meta from multiple files")

        return Meta(
            title=title,
            description=description,
            file=file_name,  # TODO: FINISH
            file_size=file_size,
            pin_hash=version.hash,
            created=version.render_created(),
            type=type,
            api_version=DEFAULT_API_VERSION,
            name=name,
            user=user if user is not None else {},
            version=version,
        )

    def create_raw(self, files: Sequence[StrOrFile], type: str, name: str) -> MetaRaw:
        return MetaRaw(files, type, name)

    def read_pin_yaml(
        self,
        f: IOBase,
        pin_name: str,
        version: "str | VersionRaw",
        local=None,
    ) -> Meta:
        if isinstance(version, str):
            version_obj = guess_version(version)
        else:
            version_obj = version

        data = yaml.safe_load(f)

        api_version = data.get("api_version", 0)
        if api_version >= 2:
            raise NotImplementedError(
                f"api_version {api_version} by this version of the pins library"
            )
        elif api_version == 0:
            cls_meta = MetaV0
        else:
            cls_meta = Meta

        return cls_meta.from_pin_dict(data, pin_name, version=version_obj, local=local)
