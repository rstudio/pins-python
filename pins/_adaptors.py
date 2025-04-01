from __future__ import annotations

import json
from abc import abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, overload

from databackend import AbstractBackend

if TYPE_CHECKING:
    import pandas as pd

    PandasDataFrame: TypeAlias = pd.DataFrame
    DataFrame: TypeAlias = PandasDataFrame


class AbstractPandasFrame(AbstractBackend):
    _backends = [("pandas", "DataFrame")]


AbstractDF: TypeAlias = AbstractPandasFrame


class Adaptor:
    _d: ClassVar[Any]

    def __init__(self, data: Any) -> None:
        self._d = data

    @overload
    def write_json(self, file: str) -> None: ...
    @overload
    def write_json(self, file: None = ...) -> str: ...
    def write_json(self, file: str | None = None) -> str | None:
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
        # TODO(compat): title says CSV rather than data.frame
        # see https://github.com/machow/pins-python/issues/5
        return f"{name}: a pinned {self._obj_name}"

    @property
    def _obj_name(self) -> str:
        return f"{type(self._d).__qualname__} object"


class DFAdaptor(Adaptor):
    _d: ClassVar[DataFrame]

    def __init__(self, data: DataFrame) -> None:
        super().__init__(data)

    @property
    def df_type(self) -> str:
        # Consider over-riding this for specialized dataframes
        return "DataFrame"

    @property
    @abstractmethod
    def columns(self) -> list[Any]: ...

    @property
    @abstractmethod
    def shape(self) -> tuple[int, int]: ...

    @abstractmethod
    def head(self, n: int) -> DFAdaptor: ...

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

    @property
    def _obj_name(self) -> str:
        row, col = self.shape
        return f"{row} x {col} {self.df_type}"


class PandasAdaptor(DFAdaptor):
    _d: ClassVar[PandasDataFrame]

    def __init__(self, data: AbstractPandasFrame) -> None:
        super().__init__(data)

    @property
    def columns(self) -> list[Any]:
        return self._d.columns.tolist()

    @property
    def shape(self) -> tuple[int, int]:
        return self._d.shape

    def head(self, n: int) -> PandasAdaptor:
        return PandasAdaptor(self._d.head(n))

    @overload
    def write_json(self, file: str) -> None: ...
    @overload
    def write_json(self, file: None) -> str: ...
    def write_json(self, file: str | None = None) -> str | None:
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
def create_adaptor(obj: DataFrame) -> DFAdaptor: ...
@overload
def create_adaptor(obj: Any) -> Adaptor: ...
def create_adaptor(obj: Any | DataFrame) -> Adaptor | DFAdaptor:
    if isinstance(obj, AbstractPandasFrame):
        return PandasAdaptor(obj)
    else:
        return Adaptor(obj)
