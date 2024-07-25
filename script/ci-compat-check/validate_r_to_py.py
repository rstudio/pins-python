import sys

from pins import board_folder, data

path_py, path_r = sys.argv[1], sys.argv[2]

# create board ----

board_py = board_folder(path_py)
board_r = board_folder(path_r)


# check pins ----

print("Checking mtcars pin")

res_mtcars = board_r.pin_read("mtcars")
assert res_mtcars.equals(data.mtcars)

meta_mtcars_py = board_py.pin_meta("mtcars")
print("\nPython meta:\n")
print(meta_mtcars_py)

meta_mtcars_r = board_r.pin_meta("mtcars")
print("\nR meta:\n")
print(meta_mtcars_r)
