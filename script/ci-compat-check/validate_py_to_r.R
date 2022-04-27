library(pins)

args <- commandArgs(trailingOnly=TRUE)


# create board ----

board_py <- board_folder(args[1])
board_r <- board_folder(args[2])


# check pins ----

cat("Checking mtcars pin\n")

res_mtcars <- board_py %>% pin_read("mtcars")
stopifnot(all.equal(res_mtcars, datasets::mtcars, check.attributes=FALSE))

meta_mtcars_py <- board_py %>% pin_meta("mtcars")
cat("\nPython meta:\n\n")
print(meta_mtcars_py)

meta_mtcars_r <- board_r %>% pin_meta("mtcars")
cat("\nR meta:\n\n")
print(meta_mtcars_r)
