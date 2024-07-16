# pins-python

## Development

### Install pins with dev dependencies

```shell
python -m pip install -e .[dev]
```

### Install pre-commit hooks

This project uses [pre-commit](https://pre-commit.com/) to check and format each commit.

You can set it up by running the following code in this repo:

```
python -m pip install pre-commit
pre-commit install
```

### Setting version number

This project uses [setuptools_scm](https://github.com/pypa/setuptools_scm) to
automatically track and change version numbers within the `pins` package.
It works by checking the last tagged commit.

In order to set the version number, create a tag like the following.

```shell
git tag v0.0.1
```

In order to see the version number being used for the current commit, run:

```
python -m setuptools_scm
```

## Test

Tests can be run using pytest:

```shell
pytest pins

# run all tests except those for Rstudio Connect
pytest pins -m 'not fs_rsc'

# run only local filesystem backend tests
pytest pins -m 'fs_file'

# run all tests except those for S3 and GCS
pytest pins -m 'not fs_s3 and not fs_gcs'

# run all tests except those using data on GitHub
# n.b. doctests cannot have marks https://github.com/pytest-dev/pytest/issues/5794
pytest pins -m 'not skip_on_github' -k 'not pins.boards.BoardManual'
```

There are two important details to note for testing:

* **Backends**. pins can write to backends like s3, azure, and RStudio Connect, so you
    will need to set credentials to test against them.
* **Pytest Marks**. You can disable tests over a specific backend through pytest's
    `-m` flag. For example...
  - Skip S3: `pytest pins -m 'not fs_s3'`
  - Test only s3: `pytest pins -m 'fs_s3'`
  - List all marks: `pytest pins --markers`

### Configuring backends

* Copy `.env.dev` to be `.env`
* Modify `.env` to file in environment variables (e.g. AWS_ACCESS_KEY_ID)
* Be careful not to put any sensitive information in `.env.dev`!

### Setting up RStudio Connect tests

```
# Be sure to set RSC_LICENSE in .env
make dev
```
