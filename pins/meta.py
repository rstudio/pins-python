from dataclasses import dataclass, asdict, field
from datetime import datetime

import yaml

from typing import Mapping, Union, Sequence, Optional
from io import IOBase

from pins.versions import Version

META_FILENAME = "data.txt"


@dataclass
class Meta:
    title: str
    description: str

    # TODO(defer): different from R pins, which has a local field
    version_name: str
    created: datetime
    file: Union[str, Sequence[str]]
    file_size: int
    pin_hash: str
    type: str

    api_version: int

    name: Optional[str] = None
    user: Mapping = field(default_factory=dict)

    def to_dict(self) -> Mapping:
        return asdict(self)

    def get_version_name(self):
        return self.version.version

    def to_yaml(self, f: IOBase):
        data = self.to_dict()
        del data["version"]

        yaml.dump(data, f)


class MetaLoader:
    def get_meta_name(self, boad, name, version):
        return META_FILENAME

    def get_version_for_meta(self, api_version):
        if api_version != 1:
            raise NotImplementedError("Unsupported api_version: %s" % api_version)

        return Version

    def load(self, f: IOBase):
        data = yaml.safe_load(f)

        version_cls = self.get_version_for_meta(data["api_version"])
        created = version_cls.parse_created(data["created"])
        version_str = version_cls.version_name(created, data["pin_hash"])

        return Meta(**{**data, "created": created, "version_name": version_str})
