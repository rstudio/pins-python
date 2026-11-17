# Deploy a data dictionary pin

This example prepares a pin bundle from synthetic data for Posit Connect.
The publisher runs the `data-dict` CLI while preparing the bundle and stores the rendered dictionary with the pin.
The combined entrypoint keeps the standard pin preview available alongside the dictionary view.
The `create_bundle.py` script uses `CONNECT_SERVER`, which defaults to `https://connect.example.com`, so it can build the bundle offline.
The rsconnect-python package publishes the bundle through the saved account named by `CONNECT_ACCOUNT`.

Run from the repository root:

```{.bash filename="Terminal"}
uv run --no-project --with-editable '.[data-dict]' python examples/data-dict/create_bundle.py /tmp/pins-python-data-dict-bundle --dictionary examples/data-dict/data-dict.yaml
uv tool run --from rsconnect-python rsconnect list
env -u CONNECT_SERVER uv tool run --from rsconnect-python rsconnect deploy manifest /tmp/pins-python-data-dict-bundle/manifest.json --name "$CONNECT_ACCOUNT" --new --title 'Pins Python data dictionary' --no-metadata
```

The output directory must be empty.
Set `CONNECT_ACCOUNT` to a saved account name before publishing, and register the account with `rsconnect login` if needed.
The deployment uses the server's default private content access.

To build a regular pin preview without a dictionary, run:

```{.bash filename="Terminal"}
uv run --no-project --with-editable . --with pyarrow python examples/data-dict/create_bundle.py /tmp/pins-python-preview-bundle --no-dictionary
```

Supply `--dictionary /path/to/data-dict.yaml` to attach an authored dictionary.
Omit `--dictionary` to generate a draft.
The dictionary must omit table `source` fields so rendering does not depend on files on the publisher's machine.
Attaching an authored dictionary to a folder board does not require the CLI.
Drafting and Connect rendering require the optional dependency or a `data-dict` executable on `PATH`.
For a pin with one Parquet file and one dictionary table, rendering profiles the pinned data and embeds column statistics in the dictionary view.
The stored dictionary omits source paths; the renderer adds the bundle-relative Parquet path to a temporary copy.
You can also supply YAML text in `metadata["data_dict"]`, using one dictionary input per call.

For an authenticated Connect board, use the public API:

```{.python filename="example.py"}
board.pin_write(data, name="username/pin-name", type="parquet", data_dict=True)
board.pin_write(data, name="username/pin-name", type="parquet", data_dict="data-dict.yaml", force_identical_write=True)
```

Dictionary-only changes need `force_identical_write=True` because pins compares data hashes when deciding whether to write.
Generated dictionaries contain `todo` notes for human decisions; review the draft before relying on it.

The CLI is optional and the supported version is `0.0.3`.
Ordinary pins do not require it.
Generated dictionary pages block executable links and remove links with unsafe URL schemes.
Dictionary-only edits require the force option; skipped writes do not draft or render a dictionary.
Install the optional extra with `python -m pip install "pins[data-dict]"`, or provide an executable on `PATH`.
