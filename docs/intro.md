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

The pins package publishes data, models, and other R objects, making it easy to share them across projects and with your colleagues.
You can pin objects to a variety of pin *boards*, including folders (to share on a networked drive or with services like DropBox), RStudio Connect, Amazon S3, Azure storage and ~Microsoft 365 (OneDrive and SharePoint)~.
Pins can be automatically versioned, making it straightforward to track changes, re-run analyses on historical data, and undo mistakes.

## 🚧 Installation

To try out the development version of pins you'll need to install from GitHub:

```shell
python -m pip install git+https://github.com/machow/pins-python
```

## Usage

To use the pins package, you must first create a pin board.
A good place to start is `board_folder()`, which stores pins in a directory you specify.
Here I'll use a special version of `board_folder()` called `board_temp()` which creates a temporary board that's automatically deleted when your R session ends.
This is great for examples, but obviously you shouldn't use it for real work!

```{code-cell} ipython3
from pins import board_temp
from pins.data import mtcars

board = board_temp()
board
```

You can "pin" (save) data to a board with `pin_write()`.
It takes three arguments: the board to pin to, an object, and a name:

```{code-cell} ipython3
board.pin_write(mtcars.head(), "mtcars", type="csv")
```

~As you can see, the data saved as an `.rds` by default~, but depending on what you're saving and who else you want to read it, you might use the `type` argument to instead save it as a `csv`, ~`json`, or `arrow`~ file.

You can later retrieve the pinned data with `pin_read()`:

```{code-cell} ipython3
board.pin_read("mtcars")
```

A board on your computer is good place to start, but the real power of pins comes when you use a board that's shared with multiple people.
To get started, you can use `board_folder()` with a directory on a shared drive or in dropbox, or if you use [RStudio Connect](https://www.rstudio.com/products/connect/) you can use `board_rsconnect()`:

🚧 TODO: add informational messages shown in display below

+++

```python
from pins import board_rsconnect

board = board_rsconnect()
#> Connecting to RSC 1.9.0.1 at <https://connect.rstudioservices.com>

board.pin_write(tidy_sales_data, "sales-summary", type = "csv")
#> Writing to pin 'hadley/sales-summary'
```

+++

Then, someone else (or an automated Rmd report) can read and use your pin:

+++

```python
board = board_rsconnect()
board.pin_read("hadley/sales-summary")
```

+++

You can easily control who gets to access the data using the RStudio Connect permissions pane.

The pins package also includes boards that allow you to share data on services like Amazon's S3 (`board_s3()`), Azure's blob storage (`board_azure()`), and Microsoft SharePoint (`board_ms365()`).
Learn more in [getting started](getting_started.Rmd).
