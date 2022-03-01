from pathlib import Path
from pins.rsconnect_api import PinBundleManifest

p = Path("pins/tests/example-bundle") / "index.html"
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text("<html><body>yo</body></html>")
PinBundleManifest.add_manifest_to_directory("pins/tests/example-bundle")
