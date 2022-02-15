from dataclasses import dataclass, asdict
from datetime import datetime
from xxhash import xxh64

from typing import Union, Sequence, Optional, Mapping
from io import IOBase

from .errors import PinsVersionError

VERSION_TIME_FORMAT = "%Y%m%dT%H%M%SZ"

StrOrFile = Union[str, IOBase]


@dataclass
class VersionRaw:
    version: str
    created: Optional[datetime] = None
    hash: Optional[str] = None

    def to_dict(self) -> Mapping:
        return asdict(self)


class Version(VersionRaw):
    version: str
    created: datetime
    hash: str

    @staticmethod
    def version_name(created, hash) -> str:
        date_part = created.strftime(VERSION_TIME_FORMAT)
        hash_part = hash[:5]
        return f"{date_part}-{hash_part}"

    @staticmethod
    def parse_created(x):
        return datetime.strptime(x, VERSION_TIME_FORMAT)

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

        return cls(version, created, hash_)

    @classmethod
    def from_files(
        cls, files: Sequence[StrOrFile], created: Union[datetime, None] = None
    ) -> "Version":
        hashes = []
        for f in files:
            hash_ = cls.hash_file(open(f, "rb") if isinstance(f, str) else f)
            hashes.append(hash_)

        if created is None:
            created = datetime.now().strftime(VERSION_TIME_FORMAT)

        if len(hashes) > 1:
            raise NotImplementedError("Only 1 file may be currently be hashed")

        return cls(cls.version_name(created, hashes), created, hashes[0])


def guess_version(x: str):
    try:
        return Version.from_string(x)
    except PinsVersionError:
        return VersionRaw(x)
