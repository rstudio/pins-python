"""Build a pin bundle from this checkout for deployment with rsconnect-python."""

import argparse
import json
import os
import tempfile
from pathlib import Path

import pandas as pd
import yaml

from pins import board_connect, board_folder
from pins.rsconnect.fs import PinBundleManifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    dictionary_options = parser.add_mutually_exclusive_group()
    dictionary_options.add_argument("--dictionary", type=Path)
    dictionary_options.add_argument("--no-dictionary", action="store_true")
    args = parser.parse_args()
    if args.no_dictionary:
        data_dict = None
    elif args.dictionary is not None:
        data_dict = args.dictionary
    else:
        data_dict = True
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        parser.error("The output directory must be empty.")

    data = pd.DataFrame(
        {"id": [1, 2, 3], "value": [10.0, 20.0, 30.0], "group": ["a", "b", "a"]}
    )
    with tempfile.TemporaryDirectory() as temporary:
        local = board_folder(temporary)
        meta = local.pin_write(
            data,
            "dictionary-example",
            type="parquet",
            metadata={"example_language": "Python"},
            data_dict=data_dict,
        )
        pd.testing.assert_frame_equal(local.pin_read("dictionary-example"), data)
        assert meta.user["example_language"] == "Python"
        dictionary = meta.user.get("data_dict")
        if dictionary is not None:
            assert all(
                "source" not in table for table in yaml.safe_load(dictionary)["tables"]
            )

        # Prepare the normal Connect bundle offline; the deployment CLI handles OAuth.
        connect = board_connect(
            server_url=os.environ.get("CONNECT_SERVER", "https://connect.example.com"),
            api_key="unused-offline",
            cache=None,
        )
        output.mkdir(parents=True, exist_ok=True)
        prepared = connect.prepare_pin_version(
            output,
            data,
            "example/data-dict-python",
            type="parquet",
            title="Pins Python data dictionary",
            metadata=meta.user,
        )
        PinBundleManifest.add_manifest_to_directory(output)
        uploaded = yaml.safe_load((output / "data.txt").read_text())
        if dictionary is None:
            assert "data_dict" not in uploaded["user"]
        else:
            assert uploaded["user"]["data_dict"] == dictionary
        pd.testing.assert_frame_equal(pd.read_parquet(output / prepared.file), data)
        manifest = json.loads((output / "manifest.json").read_text())
        assert manifest["metadata"]["primary_html"] == "index.html"
        if dictionary is not None:
            assert "Data dictionary" in (output / "index.html").read_text()
        assert "unused-offline" not in "".join(
            p.read_text() for p in output.glob("*.txt")
        )
    print(output / "manifest.json")


if __name__ == "__main__":
    main()
