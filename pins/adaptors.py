from __future__ import annotations

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


class _DFAdaptor(_Adaptor):
    _d: ClassVar[_DataFrame]

    def __init__(self, data: _DataFrame) -> None:
        super().__init__(data)

    @property
    @abstractmethod
    def columns(self) -> list[Any]: ...

    @abstractmethod
    def head(self, n: int) -> Self: ...

    @abstractmethod
    def write_json(self) -> str:
        """Write the dataframe to a JSON string.

        In the format: list like [{column -> value}, ... , {column -> value}]
        """


class _PandasAdaptor(_DFAdaptor):
    def __init__(self, data: _AbstractPandasFrame) -> None:
        super().__init__(data)

    @property
    def columns(self) -> list[Any]:
        return self._d.columns

    def head(self, n: int) -> Self:
        return _PandasAdaptor(self._d.head(n))

    def write_json(self) -> str:
        return self._d.to_json(orient="records")


@overload
def _create_df_adaptor(df: _DataFrame) -> _DFAdaptor: ...
@overload
def _create_df_adaptor(df: _PandasDataFrame) -> _PandasAdaptor: ...
def _create_df_adaptor(df):
    if isinstance(df, _AbstractPandasFrame):
        return _PandasAdaptor(df)

    msg = f"Could not determine dataframe adaptor for {df}"
    raise NotImplementedError(msg)
