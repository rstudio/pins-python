from dataclasses import dataclass, asdict
from datetime import datetime
from xxhash import xxh64

from typing import Union, Sequence, Mapping

from .errors import PinsVersionError
from ._types import StrOrFile, IOBase

VERSION_TIME_FORMAT = "%Y%m%dT%H%M%SZ"


class _VersionBase:
    pass


@dataclass
class VersionRaw(_VersionBase):
    version: str

    def to_dict(self) -> Mapping:
        return asdict(self)


@dataclass
class Version(_VersionBase):
    created: datetime
    hash: str

    def to_dict(self) -> Mapping:
        # properties not automatically added, so need to handle manually
        res = asdict(self)
        res["version"] = self.version

        return res

    @property
    def version(self) -> str:
        date_part = self.created.strftime(VERSION_TIME_FORMAT)
        hash_part = self.hash[:5]
        return f"{date_part}-{hash_part}"

    @staticmethod
    def parse_created(x):
        return datetime.strptime(x, VERSION_TIME_FORMAT)

    def render_created(self):
        return self.created.strftime(VERSION_TIME_FORMAT)

    @staticmethod
    def hash_file(f: IOBase, block_size: int = -1) -> str:
        # TODO: what kind of things implement the "buffer API"?
        hasher = xxh64()

        buf = f.read(block_size)

        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(block_size)

        return hasher.hexdigest()

    @classmethod
    def from_string(cls, version: str) -> "Version":
        parts = version.split("-")

        if len(parts) != 2:
            raise PinsVersionError(
                "version string can only have 1 '-', but contains %s" % len(parts)
            )

        dt_string, hash_ = parts

        # TODO: the datetime from pins is not timezone aware, but it looks like
        # R pins parses as UTC, then unsets the UTC part?
        try:
            created = cls.parse_created(dt_string)
        except ValueError:
            raise PinsVersionError("Invalid date part of version: " % dt_string)

        obj = cls(created, hash_)

        if obj.version != version:
            raise ValueError(
                "Version parsing failed. Received version string {version}, but "
                "output version is {cls.version}."
            )

        return obj

    @classmethod
    def from_files(
        cls, files: Sequence[StrOrFile], created: Union[datetime, None] = None
    ) -> "Version":
        hashes = []
        for f in files:
            hash_ = cls.hash_file(open(f, "rb") if isinstance(f, str) else f)
            hashes.append(hash_)

        if created is None:
            created = datetime.now()

        if len(hashes) > 1:
            raise NotImplementedError("Only 1 file may be currently be hashed")

        return cls(created, hashes[0])

    @classmethod
    def from_meta_fields(cls, created: str, hash: str):
        created_dt = cls.parse_created(created)
        return cls(created_dt, hash)


def guess_version(x: str):
    try:
        return Version.from_string(x)
    except PinsVersionError:
        return VersionRaw(x)
