import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_index_equal

from pins.adaptors import _AbstractPandasFrame, _create_df_adaptor, _PandasAdaptor


class TestCreateDFAdaptor:
    def test_pandas(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = _create_df_adaptor(df)
        assert isinstance(adaptor, _PandasAdaptor)

    def test_non_df(self):
        with pytest.raises(NotImplementedError):
            _create_df_adaptor(42)


class TestPandasAdaptor:
    def test_columns(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = _create_df_adaptor(df)
        assert_index_equal(adaptor.columns, pd.Index(["a", "b"]))

    def test_head(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = _create_df_adaptor(df)
        head1_df = pd.DataFrame({"a": [1], "b": [4]})
        expected = _create_df_adaptor(head1_df)
        assert isinstance(adaptor.head(1), _PandasAdaptor)
        assert_frame_equal(adaptor.head(1)._d, expected._d)

    def test_write_json(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = _create_df_adaptor(df)
        assert adaptor.write_json() == """[{"a":1,"b":4},{"a":2,"b":5},{"a":3,"b":6}]"""


class TestAbstractBackends:
    class TestAbstractPandasFrame:
        def test_isinstance(self):
            df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
            assert isinstance(df, _AbstractPandasFrame)

        def test_not_isinstance(self):
            assert not isinstance(42, _AbstractPandasFrame)
