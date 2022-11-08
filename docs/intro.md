---
jupytext:
  formats: md:myst
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.13.6
kernelspec:
  display_name: venv-pins-python
  language: python
  name: venv-pins-python
---

# Welcome

```{image} logo.png
:align: right
:height: 139px
:alt: pins, a library for organizing and sharing data.
```

The pins package publishes data, models, and other Python objects, making it easy to share
You can pin objects to a variety of pin *boards*, including folders (to share on a networked drive or with services like DropBox), RStudio Connect, Amazon S3, Google Cloud Storage, and Azure Datalake.
Pins can be automatically versioned, making it straightforward to track changes, re-run analyses on historical data, and undo mistakes.

You can use pins from R as well as Python. For example, you can use one language to read a pin created with the other. Learn more about [pins for R](pins.rstudio.com).

## Installation

To install the released version from PyPI:

```shell
python -m pip install pins
```

## Usage

To use the pins package, you must first create a pin board.
A good place to start is `board_folder()`, which stores pins in a directory you specify.
Here I'll use a special version of `board_folder()` called `board_temp()` which creates a temporary board that's automatically deleted when your Python session ends.
This is great for examples, but obviously you shouldn't use it for real work!

```{code-cell} ipython3
from pins import board_temp
from pins.data import mtcars

board = board_temp()
board
```

You can "pin" (save) data to a board with the `.pin_write()` method.
It requires three arguments: an object, a name, and a pin type:

```{code-cell} ipython3
board.pin_write(mtcars.head(), "mtcars", type="csv")
```

Above, we saved the data as a CSV, but depending on
what you’re saving and who else you want to read it, you might use the
`type` argument to instead save it as a `joblib` or `arrow` file (NOTE: arrow is not yet supported).

You can later retrieve the pinned data with `.pin_read()`:

```{code-cell} ipython3
board.pin_read("mtcars")
```

A board on your computer is good place to start, but the real power of pins comes when you use a board that's shared with multiple people.
To get started, you can use `board_folder()` with a directory on a shared drive or in DropBox, or if you use [RStudio Connect](https://www.rstudio.com/products/connect/) you can use `board_rsconnect()`:

+++

```python
from pins import board_rsconnect

board = board_rsconnect()

board.pin_write(tidy_sales_data, "hadley/sales-summary", type = "csv")
#> Writing pin:
#> Name: 'hadley/sales-summary'
#> Version: ...
```

+++

Then, someone else (or an automated report) can read and use your pin:

+++

```python
board = board_rsconnect()
board.pin_read("hadley/sales-summary")
```

+++

You can easily control who gets to access the data using the RStudio Connect permissions pane.

The pins package also includes boards that allow you to share data on services like
Amazon's S3 (`board_s3()`), Google Cloud Storage (`board_gcs()`), and Azure Datalake (`board_azure()`).
Learn more in [getting started](getting_started.Rmd).
