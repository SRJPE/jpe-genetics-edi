library(sf)
library(dplyr)

# Read the KML
kml_data <- st_read("data-raw/jpe_sampling_locations.kml")

# Check what's in it
glimpse(kml_data)
st_geometry_type(kml_data)  # POINT, LINESTRING, POLYGON, etc.

kml_coords <- kml_data |>
  mutate(
    longitude = st_coordinates(geometry)[, 1],
    latitude  = st_coordinates(geometry)[, 2]
  ) |>
  st_drop_geometry()

# get location codes and descriptions from db
locations <- tbl(con, "sample_location") |>
  collect() |>
  select(code, location_name, stream_name,
         description, latitude, longitude,
         managing_agency_id,
         active) |>
  left_join(tbl(con, "agency") |>
              select(id, agency_code = code, agency_name) |>
              collect(),
            by = c("managing_agency_id" = "id")) |>
  filter(!code %in% c("CONTROL", "TEST", "TEST2")) |>
  select(agency = agency_name,
         location_code = code,
         location_name,
         stream = stream_name,
         description) |>
  left_join(kml_coords |>
              mutate(name = case_when(Name == "Battle Creek RST" ~ "BTC",
                                      Name == "Butte Creek RST" ~ "BUT",
                                      Name == "Clear Creek RST" ~ "CLR",
                                      Name == "Deer Creek RST" ~ "DER",
                                      Name == "Mill Creek RST" ~ "MIL",
                                      Name == "Delta Entry RST" ~ "DEL",
                                      Name == "Knights Landing RST" ~ "KNL",
                                      Name == "Tisdale RST" ~ "TIS",
                                      Name == "Feather RM61 RST" ~ "F61",
                                      Name == "Feather RM17 RST" ~ "F17",
                                      Name == "Yuba River RST" ~ "YUR",
                                      TRUE ~ Name)) |>
                     select(name, longitude, latitude),
            by = c("location_code" = "name")) |>
  glimpse()

write_csv(locations, "data-raw/grunID_sampling_location_metadata.csv")



agency_colors <- colorFactor(
  palette = c("#0072B2", "#E69F00", "#009E73"),
  domain  = locations$agency
)

m <- leaflet(locations) |>
  addProviderTiles("CartoDB.Positron") |>
  addWMSTiles(
    baseUrl = "https://hydro.nationalmap.gov/arcgis/services/NHDPlus_HR/MapServer/WMSServer",
    layers  = "0",
    options = WMSTileOptions(format = "image/png", transparent = TRUE)
  ) |>
  addCircleMarkers(
    lng         = ~longitude,
    lat         = ~latitude,
    fillColor = "#0072B2",
    color = "#0072B2",
    #color       = ~agency_colors(agency),
    #fillColor   = ~agency_colors(agency),
    fillOpacity = 0.9,
    opacity     = 1,
    radius      = 7,
    weight      = 1.5,
    label       = ~location_code,
    labelOptions = labelOptions(
      noHide    = TRUE,
      direction = "top",
      offset    = c(0, -15),
      style     = list(
        "font-size"        = "11px",
        "font-weight"      = "600",
        "font-family"      = "sans-serif",
        "color"            = "#222222",
        "background"       = "none",
        "border"           = "none",
        "box-shadow"       = "none",
        "padding"          = "0px"
      )
    )) |>
  #   popup = ~paste0(
  #     "<div style='font-family: sans-serif; font-size: 13px; min-width: 200px;'>",
  #     "<b style='font-size: 14px;'>", location_name, "</b><br>",
  #     "<span style='color: #555;'>", stream, "</span><br><hr style='margin: 6px 0;'>",
  #     "<b>Code:</b> ", location_code, "<br>",
  #     "<b>Agency:</b> ", agency, "<br>",
  #     "<b>Description:</b> ", description, "<br>",
  #     "<b>Coordinates:</b> ", round(latitude, 4), ", ", round(longitude, 4),
  #     "</div>"
  #   )
  # ) |>
  # addLegend(
  #   position = "bottomright",
  #   pal      = agency_colors,
  #   values   = ~agency,
  #   title    = "Agency",
  #   opacity  = 0.9
  # ) |>
  addScaleBar(position = "bottomleft") |>
  addMiniMap(
    tiles         = "CartoDB.Positron",
    position      = "topleft",
    width         = 150,
    height        = 200,
    zoomLevelOffset = -5,   # pulls back 6 zoom levels from main map to show full CA
    toggleDisplay = FALSE,
    aimingRectOptions = list(
      color   = "#0072B2",
      weight  = 2,
      opacity = 0.8
    )
  ) |>
  fitBounds(
    lng1 = min(locations$longitude),
    lat1 = min(locations$latitude),
    lng2 = max(locations$longitude),
    lat2 = max(locations$latitude)
  )

mapshot(m,
        file = "data-raw/grunID_locations_map.png",
        #file    = here::here("reports", "science_basis", "figures", "locations_map.png"),
        vwidth  = 800,
        vheight = 1100
)
