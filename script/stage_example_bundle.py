import pandas as pd

from pathlib import Path
from pins.rsconnect.fs import PinBundleManifest
from pins.meta import MetaFactory

p_root = Path("pins/tests/example-bundle")
p_root.parent.mkdir(parents=True, exist_ok=True)

p_index = p_root / "index.html"
p_index.write_text("<html><body>yo</body></html>")

p_data = p_root / "data_frame.csv"
df = pd.DataFrame({"x": [1, 2, 3]})
df.to_csv(p_data)

p_meta = p_root / "data.txt"
meta = MetaFactory().create(
    str(p_data), "csv", title="some title", name="data_frame.csv"
)
meta.to_yaml(p_meta.open("w"))

# add manifest last, since it enumerates all the files
# this lets you download them individually from rsconnect
PinBundleManifest.add_manifest_to_directory(str(p_root))
