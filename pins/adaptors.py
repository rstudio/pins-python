from __future__ import annotations

import json
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, Self, TypeAlias, overload

from ._databackend import AbstractBackend

if TYPE_CHECKING:
    import pandas as pd

    _PandasDataFrame: TypeAlias = pd.DataFrame
    _DataFrame: TypeAlias = pd.DataFrame


class _AbstractPandasFrame(AbstractBackend):
    _backends = [("pandas", "DataFrame")]


_AbstractDF: TypeAlias = _AbstractPandasFrame


class _Adaptor:
    _d: ClassVar[Any]

    def __init__(self, data: Any) -> None:
        self._d = data

    @overload
    def write_json(self, file: str) -> None: ...
    @overload
    def write_json(self, file: None) -> str: ...
    def write_json(self, file=None):
        if file is None:
            msg = (
                f"Writing to JSON string rather than file is not supported for "
                f"{type(self._d)}"
            )
            raise NotImplementedError(msg)

        import json

        json.dump(self._d, open(file, mode="w"))

    def write_joblib(self, file: str) -> None:
        import joblib

        joblib.dump(self._d, file)

    def write_csv(self, file: str) -> None:
        msg = f"Writing to CSV is not supported for {type(self._d)}"
        raise NotImplementedError(msg)

    def write_parquet(self, file: str) -> None:
        msg = f"Writing to Parquet is not supported for {type(self._d)}"
        raise NotImplementedError(msg)

    def write_feather(self, file: str) -> None:
        msg = f"Writing to Feather is not supported for {type(self._d)}"
        raise NotImplementedError(msg)

    @property
    def data_preview(self) -> str:
        # note that the R library uses jsonlite::toJSON
        import json

        # TODO(compat): set display none in index.html
        return json.dumps({})

    def default_title(self, name: str) -> str:
        return f"{name}: a pinned {self._obj_name} object"

    @property
    def _obj_name(self) -> str:
        return type(self._d).__qualname__


class _DFAdaptor(_Adaptor):
    _d: ClassVar[_DataFrame]

    def __init__(self, data: _DataFrame) -> None:
        super().__init__(data)

    @property
    @abstractmethod
    def columns(self) -> list[Any]: ...

    @property
    @abstractmethod
    def shape(self) -> tuple[int, int]: ...

    @abstractmethod
    def head(self, n: int) -> Self: ...

    @property
    def data_preview(self) -> str:
        # TODO(compat) is 100 hard-coded?
        # Note that we go df -> json -> dict, to take advantage of type conversions in the dataframe library
        data: list[dict[Any, Any]] = json.loads(self.head(100).write_json())
        columns = [
            {"name": [col], "label": [col], "align": ["left"], "type": [""]}
            for col in self.columns
        ]

        # this reproduces R pins behavior, by omitting entries that would be null
        data_no_nulls = [{k: v for k, v in row.items() if v is not None} for row in data]

        return json.dumps({"data": data_no_nulls, "columns": columns})

    def default_title(self, name: str) -> str:
        # TODO(compat): title says CSV rather than data.frame
        # see https://github.com/machow/pins-python/issues/5
        shape_str = " x ".join(map(str, self.shape))
        return f"{name}: a pinned {shape_str} DataFrame"


class _PandasAdaptor(_DFAdaptor):
    def __init__(self, data: _AbstractPandasFrame) -> None:
        super().__init__(data)

    @property
    def columns(self) -> list[Any]:
        return self._d.columns.tolist()

    @property
    def shape(self) -> tuple[int, int]:
        return self._d.shape

    def head(self, n: int) -> Self:
        return _PandasAdaptor(self._d.head(n))

    @overload
    def write_json(self, file: str) -> None: ...
    @overload
    def write_json(self, file: None) -> str: ...
    def write_json(self, file=None):
        if file is not None:
            msg = (
                f"Writing to file rather than JSON string is not supported for "
                f"{type(self._d)}"
            )
            raise NotImplementedError(msg)

        return self._d.to_json(orient="records")

    def write_csv(self, file: str) -> None:
        self._d.to_csv(file, index=False)

    def write_parquet(self, file: str) -> None:
        self._d.to_parquet(file)

    def write_feather(self, file: str) -> None:
        self._d.to_feather(file)


@overload
def _create_adaptor(obj: Any) -> _Adaptor: ...
@overload
def _create_adaptor(obj: _DataFrame) -> _DFAdaptor: ...
@overload
def _create_adaptor(obj: _PandasDataFrame) -> _PandasAdaptor: ...
def _create_adaptor(obj):
    if isinstance(obj, _AbstractPandasFrame):
        return _PandasAdaptor(obj)
    else:
        return _Adaptor(obj)
