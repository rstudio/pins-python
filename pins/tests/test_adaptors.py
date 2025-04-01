from pathlib import Path

import joblib
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from pins._adaptors import (
    AbstractPandasFrame,
    Adaptor,
    DFAdaptor,
    PandasAdaptor,
    create_adaptor,
)


class TestCreateAdaptor:
    def test_pandas(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = create_adaptor(df)
        assert isinstance(adaptor, Adaptor)
        assert isinstance(adaptor, PandasAdaptor)

    def test_non_df(self):
        adaptor = create_adaptor(42)
        assert isinstance(adaptor, Adaptor)
        assert not isinstance(adaptor, PandasAdaptor)
        assert not isinstance(adaptor, DFAdaptor)


class TestAdaptor:
    def test_write_json(self, tmp_path: Path):
        data = {"a": 1, "b": 2}
        adaptor = Adaptor(data)
        file = tmp_path / "file.json"
        adaptor.write_json(file)
        assert file.read_text() == '{"a": 1, "b": 2}'

    def test_write_joblib(self, tmp_path: Path):
        data = {"a": 1, "b": 2}
        adaptor = Adaptor(data)
        file = tmp_path / "file.joblib"
        adaptor.write_joblib(file)

        # Dump independently and check contents
        expected_file = tmp_path / "expected.joblib"
        joblib.dump(data, expected_file)
        assert expected_file.read_bytes() == file.read_bytes()

    def test_write_csv(self):
        with pytest.raises(NotImplementedError):
            adaptor = Adaptor(42)
            adaptor.write_csv("file.csv")

    def test_write_parquet(self):
        with pytest.raises(NotImplementedError):
            adaptor = Adaptor(42)
            adaptor.write_parquet("file.parquet")

    def test_write_feather(self):
        with pytest.raises(NotImplementedError):
            adaptor = Adaptor(42)
            adaptor.write_feather("file.feather")

    class TestDataPreview:
        def test_int(self):
            adaptor = Adaptor(42)
            assert adaptor.data_preview == "{}"

        def test_dict(self):
            data = {"a": 1, "b": 2}
            adaptor = Adaptor(data)
            assert adaptor.data_preview == "{}"

    def test_default_title(self):
        adaptor = Adaptor(42)
        assert adaptor.default_title("my_data") == "my_data: a pinned int object"


class TestPandasAdaptor:
    def test_df_type(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = PandasAdaptor(df)
        assert adaptor.df_type == "DataFrame"

    def test_columns(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = PandasAdaptor(df)
        assert isinstance(adaptor, DFAdaptor)
        assert isinstance(adaptor, PandasAdaptor)
        assert adaptor.columns == ["a", "b"]

    def test_shape(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = PandasAdaptor(df)
        assert isinstance(adaptor, DFAdaptor)
        assert isinstance(adaptor, PandasAdaptor)
        assert adaptor.shape == (3, 2)

    def test_head(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = PandasAdaptor(df)
        head1_df = pd.DataFrame({"a": [1], "b": [4]})
        expected = create_adaptor(head1_df)
        assert isinstance(adaptor, DFAdaptor)
        assert isinstance(adaptor.head(1), DFAdaptor)
        assert isinstance(adaptor.head(1), PandasAdaptor)
        assert_frame_equal(adaptor.head(1)._d, expected._d)

    def test_write_json(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = PandasAdaptor(df)
        assert isinstance(adaptor, DFAdaptor)
        assert adaptor.write_json() == """[{"a":1,"b":4},{"a":2,"b":5},{"a":3,"b":6}]"""

    def test_write_csv(self, tmp_path: Path):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = PandasAdaptor(df)
        file = tmp_path / "file.csv"
        adaptor.write_csv(file)
        assert file.read_text() == "a,b\n1,4\n2,5\n3,6\n"

    def test_write_parquet(self, tmp_path: Path):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = PandasAdaptor(df)
        file = tmp_path / "file.parquet"
        adaptor.write_parquet(file)
        assert_frame_equal(pd.read_parquet(file), df)

    def test_write_feather(self, tmp_path: Path):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = PandasAdaptor(df)
        file = tmp_path / "file.feather"
        adaptor.write_feather(file)
        assert_frame_equal(pd.read_feather(file), df)

    def test_data_preview(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = PandasAdaptor(df)
        expected = (
            '{"data": [{"a": 1, "b": 4}, {"a": 2, "b": 5}, {"a": 3, "b": 6}], '
            '"columns": [{"name": ["a"], "label": ["a"], "align": ["left"], "type": [""]}, '
            '{"name": ["b"], "label": ["b"], "align": ["left"], "type": [""]}]}'
        )
        assert adaptor.data_preview == expected

    def test_default_title(self):
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
        adaptor = PandasAdaptor(df)
        assert adaptor.default_title("my_df") == "my_df: a pinned 3 x 2 DataFrame"


class TestAbstractBackends:
    class TestAbstractPandasFrame:
        def test_isinstance(self):
            df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})
            assert isinstance(df, AbstractPandasFrame)

        def test_not_isinstance(self):
            assert not isinstance(42, AbstractPandasFrame)
