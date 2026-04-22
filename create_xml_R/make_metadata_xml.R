library(EDIutils)
library(EMLaide)
library(tidyverse)
library(readxl)
library(EML)

datatable_metadata <-
  dplyr::tibble(filepath = c("data/genetics_results.csv"),
                attribute_info = c("data-raw/metadata/genetics_results_metadata.xlsx"),
                datatable_description = c("Genetic results"),
                datatable_url = paste0("https://raw.githubusercontent.com/SRJPE/jpe-genetics-edi/main/data/",
                                       c("genetics_results.csv")))

excel_path <- "data-raw/metadata/genetics_project_metadata.xlsx"
sheets <- readxl::excel_sheets(excel_path)
metadata <- lapply(sheets, function(x) readxl::read_excel(excel_path, sheet = x))
names(metadata) <- sheets

abstract_docx <- "data-raw/metadata/abstract.docx"
methods_docx <- "data-raw/metadata/methods.md"

#edi_number <- reserve_edi_id(user_id = Sys.getenv("edi_user_id"), password = Sys.getenv("edi_password"))
edi_number <- "genetics" # placeholder

dataset <- list() %>%
  add_pub_date() %>%
  add_title(metadata$title) %>%
  add_personnel(metadata$personnel) %>%
  add_keyword_set(metadata$keyword_set) %>%
  add_abstract(abstract_docx) %>%
  add_license(metadata$license) %>%
  add_method(methods_docx) %>%
  add_maintenance(metadata$maintenance) %>%
  add_project(metadata$funding) %>%
  add_coverage(metadata$coverage, metadata$taxonomic_coverage) %>%
  add_datatable(datatable_metadata)

# GO through and check on all units
# not necessary right now
# custom_units <- data.frame(id = c(NA),
#                            unitType = c("dimensionless"),
#                            parentSI = c(NA),
#                            multiplierToSI = c(NA),
#                            description = c(NA))


#unitList <- EML::set_unitList(custom_units)

edi_number
eml <- list(packageId = edi_number,
            system = "EDI",
            access = add_access(),
            dataset = dataset) #,
#             additionalMetadata = list(metadata = list(unitList = unitList))
# )
edi_number
EML::write_eml(eml, paste0(edi_number, ".xml"))
EML::eml_validate(paste0(edi_number, ".xml"))

# evaluate and upload package
# EMLaide::evaluate_edi_package(Sys.getenv("edi_user_id"), Sys.getenv("edi_password"), paste0(edi_number, ".xml"))
# EMLaide::upload_edi_package(Sys.getenv("edi_user_id"), Sys.getenv("edi_password"), paste0(edi_number, ".xml"))
