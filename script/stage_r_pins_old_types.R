cache = tempfile()
board_register_local(cache = cache)

some_df = data.frame(a = 1:2, b = c("x","y"))
pin(some_df, name="a-table")

# note that pin automatically changes _ to -
# TODO: for now manually copying into pins/tests/pins-old-types
# Note that a trivial version name, v, is used to check the reading behavior
# since pins v0 does not save versions
# >>> mkdir pins/tests/pins-old-types/a-table/v/
# >>> cp -r <path_to_pin> pins/tests/pins-old-types/a-table/v/
fs::path(cache, "a-table")
