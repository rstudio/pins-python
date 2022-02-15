library(pins)

df <- data.frame(x = 1:2, y = c("a", "b"))
df_v2 <- data.frame(x = 1:2, y = c("a", "b"), z = 3:4)

#board <- board_s3("ci-pins", prefix = "r-pins-test")
board <- board_folder("pins/tests/pins-compat", versioned=TRUE)

all_pins <- board %>% pin_list()
board %>% pin_delete(all_pins)

# write two versions of df as CSV ----
board %>% pin_write(df, "df_csv", type="csv")
Sys.sleep(2)
board %>% pin_write(df_v2, "df_csv", type="csv")

# write two versions of df as arrow ----
board %>% pin_write(df, "df_arrow", type="arrow")

# write two versions of df as RDS ----
board %>% pin_write(df, "df_rds", type="rds")

# write unversioned pin as CSV
board %>% pin_write(df, "df_unversioned", versioned=FALSE)
