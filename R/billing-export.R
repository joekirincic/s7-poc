library(DBI)
library(tidyr)
source("R/s7-types-and-generics.R")

if (file.exists(".env")) {
  dotenv::load_dot_env()
}

billing_export <- function(type, output_dir, data) {
  config <- pluck(data, "config", 1)
  out <- switch(
    type,
    "popular-vendor" = {
      PopularVendorBillingExport(
        data = data,
        config = config,
        output_dir = output_dir
      )
    },
    "niche-vendor" = {
      NicheVendorBillingExport(
        data = data,
        config = config,
        output_dir = output_dir
      )
    }
  )
  out
}

generate_billing_export_safely <- purrr::safely(
  generate_billing_export,
  otherwise = NULL
)

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
      .f = \(x, y) {
        be <- billing_export(type = y, output_dir = TARGET_DIR, data = x)
        generate_billing_export_safely(be)
      }
    )
  )
