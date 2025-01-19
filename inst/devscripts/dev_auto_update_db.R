weatherAndClimateDatabase::fetch_weather_synoptic(
  station_ids = c("ksan","g3667"),
  date_range = lubridate::ymd(c("2025-01-01", "2025-01-03")),
  tzone_out = "America/Los_Angeles"
)

# 2025-01-19 Before any auto-updating feature was added, a call to
#' fetch_weather_synoptic on an invalid time resulted in an empty tibble.
#' A tibble: 0 × 8
#' ℹ 8 variables: Station_ID <chr>, Station_Name <chr>, latitude <dbl>,
#'   longitude <dbl>, elevation_ft <dbl>, state <chr>, local_timezone <chr>,
#'   obs <list>

weatherAndClimateDatabase:::fetch_weather_daily(
  station_ids = c("ksan","g3667"),
  date_range = lubridate::ymd(c("2025-01-01", "2025-01-03"))
)
# 2025-01-19 A call to fetch_weather_daily at an invalid time also returned a
#' an empty tibble
#' # A tibble: 0 × 8
# ℹ 8 variables: Station_ID <chr>, Station_Name <chr>, elevation_ft <dbl>,
#   state <chr>, local_timezone <chr>, latitude <dbl>, longitude <dbl>,
#   obs <list>

