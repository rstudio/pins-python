# flake8: noqa

# Set version ----
from importlib.metadata import version as _v

__version__ = _v("pins")

del _v


# Imports ----
from .boards import BaseBoard
