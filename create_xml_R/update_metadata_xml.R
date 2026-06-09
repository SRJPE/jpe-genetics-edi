library(EDIutils)
library(EMLaide)
library(tidyverse)
library(readxl)
library(EML)

datatable_metadata <-
  dplyr::tibble(filepath = c("create_xml_R/data/genetic_identification_data.csv"),
                attribute_info = c("create_xml_R/data-raw/metadata/genetic_identification_metadata.xlsx"),
                datatable_description = c("Results"),
                datatable_url = paste0("https://raw.githubusercontent.com/SRJPE/jpe-genetics-edi/main/create_xml_R/data/",
                                       c("genetic_identification_data.csv")))

excel_path <- "create_xml_R/data-raw/metadata/project_metadata.xlsx"
sheets <- readxl::excel_sheets(excel_path)
metadata <- lapply(sheets, function(x) readxl::read_excel(excel_path, sheet = x))
names(metadata) <- sheets

abstract_docx <- "create_xml_R/data-raw/metadata/abstract.docx"
methods_docx <- "create_xml_R/data-raw/metadata/methods.docx"

#edi_number <- reserve_edi_id(user_id = Sys.getenv("edi_user_id"), password = Sys.getenv("edi_password"))

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


# create new xml ----------------------------------------------------------
# get latest data from EDI ------------------------------------------------
scope <- "edi"
identifier <- "2335"
revision <- list_data_package_revisions(scope, identifier, filter = "newest")
existing_package_id <- paste(scope, identifier, revision, sep = ".")
new_package_id <- paste(scope, identifier, revision + 1, sep = ".")
edi_number <- existing_package_id
eml <- list(packageId = new_package_id,
            system = "EDI",
            access = add_access(),
            dataset = dataset) #,
#             additionalMetadata = list(metadata = list(unitList = unitList))
# )
edi_number
EML::write_eml(eml, paste0(edi_number, ".xml"))
EML::eml_validate(paste0(edi_number, ".xml"))

# evaluate and upload package
EMLaide::evaluate_edi_package(Sys.getenv("EDI_USER_ID"), 
                              Sys.getenv("EDI_PASSWORD"), 
                              eml_file_path = paste0(new_package_id, ".xml"))
EMLaide::update_edi_package(Sys.getenv("EDI_USER_ID"), 
                            Sys.getenv("EDI_PASSWORD"), 
                            existing_package_identifier = existing_package_id,
                            eml_file_path = paste0(new_package_id, ".xml"))
