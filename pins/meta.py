from dataclasses import dataclass, asdict, field

import yaml

from typing import Mapping, Union, Sequence, Optional

from .versions import Version
from ._types import StrOrFile, IOBase

META_FILENAME = "data.txt"
DEFAULT_API_VERSION = 1


@dataclass
class Meta:
    """Represent metadata for a pin version.

    Parameters
    ----------
    title:
        A title for the pin.
    description:
        A detailed description of the pin contents.
    file:
        All relevant files contained in the pin.
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

    """

    title: str
    description: Optional[str]

    # TODO(defer): different from R pins, which has a local field
    # created: datetime
    # pin_hash: str

    file: Union[str, Sequence[str]]
    file_size: int
    type: str

    api_version: int

    # In the metadata yaml, the created field uses a custom format, so
    # we need a version object in order to render it. You can think of
    # the version here as "the thing that was used to create version_name,
    # pin_hash, created, etc.."
    version: Version

    name: Optional[str] = None
    user: Mapping = field(default_factory=dict)

    def to_dict(self, flat=False, fmt_created=False) -> Mapping:
        data = asdict(self)

        if fmt_created:
            created_val = self.version.render_created()
        else:
            created_val = data["version"]["created"]

        if not flat:
            if fmt_created:
                data["version"]["created"] = created_val
            return data
        else:
            flat_data = {k: v for k, v in data.items() if k != "version"}

            flat_data["created"] = created_val
            flat_data["pin_hash"] = data["version"]["hash"]

            return flat_data

    def to_pin_dict(self):
        return self.to_dict(flat=True, fmt_created=True)

    @classmethod
    def from_pin_dict(cls, data, version_cls) -> "Meta":
        version_fields = {"created", "pin_hash"}

        # get items necessary for re-creating meta data
        meta_data = {k: v for k, v in data.items() if k not in version_fields}
        version = version_cls.from_meta_fields(data["created"], data["pin_hash"])
        return cls(**meta_data, version=version)

    def to_yaml(self, f: Optional[IOBase] = None) -> "str | None":
        data = self.to_dict(flat=True, fmt_created=True)

        return yaml.dump(data, f)


class MetaFactory:
    """Responsible for creating and loading (e.g. from yaml) of meta objects.

    """

    def get_meta_name(self, *args, **kwargs) -> str:
        return META_FILENAME

    def get_version_for_meta(self, api_version) -> Version:
        if api_version != 1:
            raise NotImplementedError("Unsupported api_version: %s" % api_version)

        return Version

    def create(
        self,
        files: Sequence[StrOrFile],
        type,
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
            file=name,  # TODO: FINISH
            file_size=file_size,
            type=type,
            api_version=DEFAULT_API_VERSION,
            name=name,
            user=user if user is not None else {},
            version=version,
        )

    def read_yaml(self, f: IOBase) -> Meta:
        data = yaml.safe_load(f)

        version_cls = self.get_version_for_meta(data["api_version"])
        return Meta.from_pin_dict(data, version_cls=version_cls)
