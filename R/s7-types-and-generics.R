library(S7)
library(dplyr)
library(purrr)
library(lubridate)
library(readr)

BillingExport <- new_class(
  name = "BillingExport",
  properties = list(
    data = new_property(
      class = class_data.frame
    ),
    config = new_property(
      class = class_list
    ),
    output_dir = new_property(
      class = class_character,
      default = here::here("data", "billing-export")
    ),
    file_ext = new_property(
      class = class_character
    ),
    output_file = new_property(
      getter = function(self) {
        facility <- pluck(self@data, "facility", 1)
        file.path(self@output_dir, sprintf("%s.%s", facility, self@file_ext))
      }
    ),
    errors = new_property(
      class = class_data.frame
    )
  ),
  abstract = TRUE
)

PopularVendorBillingExport <- new_class(
  name = "PopularVendorBillingExport",
  parent = BillingExport,
  properties = list(
    config = new_property(
      class = class_list,
      default = list(
        exclude_medical_code_field = FALSE,
        exclude_treatment_code_field = FALSE
      )
    ),
    file_ext = new_property(
      class = class_character,
      default = "tsv"
    )
  )
)

NicheVendorBillingExport <- new_class(
  name = "NicheVendorBillingExport",
  parent = BillingExport,
  properties = list(
    config = new_property(
      class = class_list,
      default = list(
        date_constraint = FALSE
      )
    ),
    file_ext = new_property(
      class = class_character,
      default = "csv"
    )
  )
)

generate_billing_export <- new_generic(
  name = "generate_billing_export",
  dispatch_args = "x"
)

method(generate_billing_export, PopularVendorBillingExport) <- function(
  x,
  ...
) {
  # We make a TSV file.
  d <- x@data |>
    mutate(
      medical_code = if_else(
        rep(x@config$exclude_medical_code_field, nrow(x@data)),
        #map_lgl(config, "exclude_medical_code_field"),
        "",
        medical_code
      ),
      treatment_code = if_else(
        rep(x@config$exclude_treatment_code_field, nrow(x@data)),
        #map_lgl(config, "exclude_treatment_code_field"),
        "",
        treatment_code
      )
    ) |>
    select(
      -one_of(c(
        "config",
        "billing_export_type"
      ))
    )

  d |>
    readr::write_tsv(
      file = x@output_file,
      na = ""
    )

  invisible(x)
}

method(generate_billing_export, NicheVendorBillingExport) <- function(
  x,
  ...
) {
  CURRENT_YEAR <- year(today())

  d <- x@data |>
    mutate(
      date_constraint_violated_ind = rep(
        x@config$date_constraint,
        nrow(x@data)
      ) &
        !(year(date_of_service) == CURRENT_YEAR)
    )

  validate_date <- function(d) {
    facility <- pluck(d, "facility", 1)
    bad_rows <- filter(d, date_constraint_violated_ind)
    assertthat::validate_that(
      {
        nrow(bad_rows) == 0
      },
      msg = sprintf(
        "Skipping facility %s: %i rows violate date constraint.",
        facility,
        nrow(bad_rows)
      )
    )
  }

  date_check <- validate_date(d)

  if (!isTRUE(date_check)) {
    message(date_check)
    x@errors <- d |> filter(date_constraint_violated_ind)
    return(invisible(x))
  }

  write_csv(
    select(
      d,
      -date_constraint_violated_ind,
      -config,
      -billing_export_type
    ),
    file = x@output_file,
    na = "",
    append = FALSE
  )

  invisible(x)
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
