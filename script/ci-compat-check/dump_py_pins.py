import sys

from pins.data import mtcars
from pins import board_folder

if len(sys.argv) < 2:
    raise ValueError("must pass board location as command-line argument")
else:
    BOARD_PATH = sys.argv[1]

board = board_folder(BOARD_PATH)
board.pin_write(mtcars, "mtcars", type="csv")
