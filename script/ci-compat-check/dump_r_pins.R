library(pins)
args <- commandArgs(trailingOnly=TRUE)

board <- board_folder(args[1])
board %>% pin_write(mtcars, "mtcars", type="csv")
