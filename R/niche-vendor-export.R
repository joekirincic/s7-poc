library(DBI)
library(dplyr)
library(purrr)
library(stringr)
library(lubridate)
library(readr)
library(tidyr)

dotenv::load_dot_env()

con <- dbConnect(
  drv = RPostgres::Postgres(),
  dbname = Sys.getenv("DB_NAME"),
  host = "localhost",
  port = 15432,
  user = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD")
)

CURRENT_YEAR <- year(today())

facility_billing_config <- tbl(con, "facility_billing_config") |>
  filter(
    billing_export_type == "niche-vendor"
  ) |>
  collect() |>
  mutate(
    config = map(config, ~ jsonlite::fromJSON(.x, simplifyDataFrame = FALSE))
  )

billing <- tbl(con, "billing") |>
  collect() |>
  inner_join(
    facility_billing_config,
    by = join_by(facility == facility)
  ) |>
  hoist(
    .col = config,
    onset_date_constraint_ind = "onset_date_constraint"
  ) |>
  mutate(
    onset_date_constraint_violated_ind = onset_date_constraint_ind &
      !(year(date_of_service) == CURRENT_YEAR)
  ) |>
  select(
    -one_of(c(
      "billing_export_type",
      "onset_date_constraint_ind"
    ))
  )

validate_onset_date <- function(d) {
  facility <- pluck(d, "facility", 1)
  bad_rows <- filter(d, onset_date_constraint_violated_ind)
  assertthat::validate_that(
    {
      nrow(bad_rows) == 0
    },
    msg = sprintf(
      "Skipping facility %s: %i rows violate onset date constraint.",
      facility,
      nrow(bad_rows)
    )
  )
}

TARGET_DIR <- here::here("data", "niche-vendor")
if (!dir.exists(TARGET_DIR)) {
  dir.create(TARGET_DIR)
}

billing |>
  group_by(
    facility
  ) |>
  group_walk(
    \(d, g) {
      TARGET_FILE <- file.path(
        TARGET_DIR,
        sprintf("%s.csv", tolower(g$facility))
      )
      tst <- validate_onset_date(d)
      if (!isTRUE(tst)) {
        print(tst)
        return()
      }
      write_csv(
        d,
        file = TARGET_FILE,
        na = "",
        append = FALSE
      )
    }
  )
