# pins-python

## Install

```shell
python -m pip install git+https://github.com/machow/pins-python
```

## Develop

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
