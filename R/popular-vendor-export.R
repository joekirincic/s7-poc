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

facility_billing_config <- tbl(con, "facility_billing_config") |>
  filter(
    billing_export_type == "popular-vendor"
  ) |>
  collect() |>
  mutate(
    config = map(config, ~ jsonlite::fromJSON(.x, simplifyDataFrame = FALSE))
  )

cfg <- list(
  exclude_medical_code_field = TRUE,
  exclude_treatment_code_field = TRUE
)

billing <- tbl(con, "billing") |>
  collect() |>
  inner_join(
    facility_billing_config,
    by = join_by(facility == facility)
  ) |>
  hoist(
    .col = config,
    exclude_medical_ind = "exclude_medical_code_field",
    exclude_treatment_ind = "exclude_treatment_code_field"
  ) |>
  mutate(
    medical_code = if_else(
      cfg$exclude_medical_code_field,
      "",
      medical_code
    ),
    treatment_code = if_else(
      cfg$exclude_treatment_code_field,
      "",
      treatment_code
    )
  ) |>
  select(
    -one_of(c(
      "billing_export_type",
      "exclude_medical_ind",
      "exclude_treatment_ind"
    ))
  )

TARGET_DIR <- here::here("data", "popular-vendor")
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
        sprintf("%s.tsv", tolower(g$facility))
      )
      write_tsv(
        d,
        file = TARGET_FILE,
        na = "",
        append = FALSE
      )
    }
  )
