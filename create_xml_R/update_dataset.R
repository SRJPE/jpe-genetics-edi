library(tidyverse)
library(grunID)
library(EDIutils)
library(here)

# data update 

# Pull genetics data
# Data are on the Azure database and on EDI
# Proposed strategy is to pull directly from EDI
# and have EDI be updated through pulls from database
# on a regular schedule

# edi package (seasons 2022-2025) [as of May 2026]
# Set the scope for script to use API to download data from EDI


# get latest data from EDI ------------------------------------------------
scope <- "edi"
identifier <- "2335"
revision <- list_data_package_revisions(scope, identifier, filter = "newest")
package_id <- paste(scope, identifier, revision, sep = ".")

# List data entities of the data package
res <- read_data_entity_names(package_id)
name <- "genetic_identification_data.csv"
entity_id <- res$entityId[res$entityName == name]
# download data from 2022-2025
raw <- read_data_entity(package_id, entity_id)
original_data <- read_csv(file = raw)

last_date_on_edi <- max(original_data$datetime_collected, na.rm = T)


# pull data from db -------------------------------------------------------
# requires config.yml
con <- gr_db_connect()

db_data <- generate_final_run_assignment(con)$results
last_date_on_db <- max(db_data$datetime_collected, na.rm = T)



# re-generate data file ---------------------------------------------------
# always append - will never be able to recreate 2022-2024 seasons
# using the database approach

if(last_date_on_db > last_date_on_edi) {
  data_to_append <- db_data |> 
    mutate(season = as.numeric(substr(sample_id, 4, 5)),
           coleman_f = as.numeric(coleman_f)) |> 
    filter(season > 24, 
           !sample_id %in% original_data$sample_id,
           datetime_collected <= as_datetime(Sys.Date())) |> 
    select(-season)
}


new_data_to_upload <- bind_rows(original_data, data_to_append)

write_csv(new_data_to_upload, 
          here("create_xml_R", "data", "genetic_identification_data.csv"))


# check data --------------------------------------------------------------

# update metadata
summarize_columns <- function(df) {
  for (col in names(df)) {
    cat("Column:", col, "\n")
    
    if (is.numeric(df[[col]])) {
      cat("  Min:", min(df[[col]], na.rm = TRUE), "\n")
      cat("  Max:", max(df[[col]], na.rm = TRUE), "\n")
    } else if (inherits(df[[col]], c("POSIXct", "POSIXlt", "Date"))) {
      cat("  Earliest:", format(min(df[[col]], na.rm = TRUE)), "\n")
      cat("  Latest:  ", format(max(df[[col]], na.rm = TRUE)), "\n")
    } else if (is.character(df[[col]]) || is.factor(df[[col]])) {
      cat("  Unique values:", paste(unique(df[[col]]), collapse = ", "), "\n")
    }
    
    cat("\n")
  }
}

summarize_columns(new_data_to_upload)
