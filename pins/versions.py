from dataclasses import dataclass
from datetime import datetime


class PinsException(Exception):
    pass


class PinsVersionError(PinsException):
    pass


@dataclass
class Version:
    version: str
    created: datetime
    hash: str

    @classmethod
    def from_string(cls, version: str):
        dt_string, hash_ = version.split("-")

        # TODO: the datetime from pins is not timezone aware, but it looks like
        # R pins parses as UTC, then unsets the UTC part?
        try:
            created = datetime.strptime(dt_string, "%Y%m%dT%H%M%SZ")
        except ValueError:
            raise PinsVersionError("Invalid date part of version: " % dt_string)

        return cls(version, created, hash_)

    @staticmethod
    def create_hash(x):
        raise NotImplementedError()
