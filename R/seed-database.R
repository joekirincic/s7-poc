library(fabricatr)
library(stringr)
library(lubridate)
library(dplyr)
library(purrr)
library(tidyr)
library(DBI)

con <- dbConnect(
  drv = RPostgres::Postgres(),
  dbname = Sys.getenv("DB_NAME"),
  host = Sys.getenv("DB_HOST"),
  port = Sys.getenv("DB_PORT"),
  user = Sys.getenv("DB_USER"),
  password = Sys.getenv("DB_PASSWORD")
)

billing <- fabricate(
  facility_id = add_level(N = 3, n_patients = sample(1:3, size = 1)),
  patient_id = add_level(
    n_patients,
    patient = randomNames::randomNames(n = N),
    n_dates = sample(1:3, size = N, replace = TRUE),
    n_billing_codes = sample(1:3, size = N, replace = TRUE),
    billing_codes = map_chr(
      n_billing_codes,
      ~ {
        sample(c("ABC123", "DEF456", "GHI789"), size = .x, replace = FALSE) |>
          str_flatten_comma()
      }
    )
  ),
  date_of_service_id = add_level(
    N = n_dates,
    date_of_service = sample(
      seq_len(days_in_month(today())),
      size = N,
      replace = TRUE
    ) |>
      make_date(year = 2026, month = month(today()), day = _)
  )
) |>
  separate_longer_delim(
    billing_codes,
    delim = ", "
  ) |>
  mutate(
    facility = paste0("FAC-", facility_id),
    minutes = sample(c(8, 10, 15, 30, 45, 60), size = n(), replace = TRUE)
  ) |>
  rename(
    billing_code = billing_codes
  ) |>
  transmute(
    facility,
    patient,
    date_of_service,
    billing_code,
    medical_code = "abc987",
    treatment_code = "def123",
    minutes
  ) |>
  arrange(
    facility,
    patient,
    date_of_service,
    billing_code
  )

facility_billing_config <- tibble(
  facility = c("FAC-1", "FAC-2", "FAC-3"),
  billing_export_type = c("popular-vendor", "popular-vendor", "niche-vendor"),
  config = list(
    list(
      exclude_medical_code_field = TRUE,
      exclude_treatment_code_field = TRUE
    ),
    list(
      exclude_medical_code_field = FALSE,
      exclude_treatment_code_field = FALSE
    ),
    list(
      onset_date_constraint = TRUE
    )
  )
) |>
  mutate(
    config = map_chr(config, ~ jsonlite::toJSON(.x, auto_unbox = TRUE))
  )

dbWriteTable(
  con,
  name = "billing",
  value = billing,
  overwrite = TRUE
)

dbWriteTable(
  con,
  name = "facility_billing_config",
  value = facility_billing_config,
  overwrite = TRUE,
  field.types = c("config" = "JSONB")
)

# Say we do the initial data pull. How do we get all the configuration
# info? I say there's a separate DB pull from a table consisting of
# facility_uuid, billing_export_type, and a JSONB column of configuration options.
# So something like
# | facility_uuid | billing_export_type | config |
# ------------------------------------------------
# | abc123        | PCC                 | {...}  |
# This gets joined onto the grouped tibble from the ub04
# to produce the following.
# | facility_uuid | facility | billing_export_type | config | data |
# ------------------------------------------------------------------
# | abc123        | FAC-1    | PCC                 | {...}  | data |
# From here, we'd just use `mutate` and `map` to produce the reports.
# We could even parallelize using `{purrr}`'s `in_parallel` adverb
# and {mirai} since this is now an embarrassingly parallel problem.
#
