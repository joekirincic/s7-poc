library(DBI)
library(tidyr)
library(mirai)
source("R/s7-types-and-generics.R")

daemons(n = 6L)
everywhere({
  source("R/s7-types-and-generics.R")
})

if (file.exists(".env")) {
  dotenv::load_dot_env()
}

con <- dbConnect(
  drv = RPostgres::Postgres(),
  dbname = Sys.getenv("DB_NAME"),
  host = Sys.getenv("DB_HOST"),
  port = Sys.getenv("DB_PORT"),
  user = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD")
)

facility_billing_config <- tbl(con, "facility_billing_config") |>
  collect() |>
  mutate(
    config = map(config, ~ jsonlite::fromJSON(.x, simplifyDataFrame = FALSE))
  )

TARGET_DIR <- here::here("data", "billing-export")

if (!dir.exists(TARGET_DIR)) {
  dir.create(TARGET_DIR, recursive = TRUE)
}

billing <- tbl(con, "billing") |>
  collect() |>
  left_join(
    facility_billing_config,
    by = join_by(facility == facility)
  ) |>
  group_nest(
    facility,
    billing_export_type,
    keep = TRUE
  ) |>
  transmute(
    facility,
    billing_export_type,
    billing_export = map2(
      .x = data,
      .y = billing_export_type,
      .f = in_parallel(
        \(x, y) {
          be <- billing_export(type = y, output_dir = TARGET_DIR, data = x)
          generate_billing_export_safely(be)
        },
        TARGET_DIR = TARGET_DIR
      )
    )
  )
