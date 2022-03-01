import tempfile

from pins.drivers import load_data
from pins.meta import MetaFactory

mf = MetaFactory()


class MockFs:
    def open(self, path, *args, **kwargs):
        return open(path, "rb", *args, **kwargs)


def test_driver_load_data_csv():
    import pandas as pd
    from pathlib import Path

    df = pd.DataFrame({"x": [1, 2, 3]})
    with tempfile.NamedTemporaryFile() as tmp_file:

        df.to_csv(tmp_file.file)
        tmp_file.file.close()

        meta = mf.create(
            files=tmp_file.name, type="csv", name=Path(tmp_file.name).name, title=""
        )

        data = load_data(meta, MockFs(), Path(tmp_file.name).parent)  # noqa


def test_driver_load_data_joblib():
    import pandas as pd
    import joblib
    from pathlib import Path

    df = pd.DataFrame({"x": [1, 2, 3]})
    with tempfile.NamedTemporaryFile() as tmp_file:

        joblib.dump(df, tmp_file.file)
        tmp_file.file.close()

        meta = mf.create(
            files=tmp_file.name, type="joblib", name=Path(tmp_file.name).name, title=""
        )

        data = load_data(meta, MockFs(), Path(tmp_file.name).parent)  # noqa
