#' Fetch subsets of weather data from the databases
#'
#' @param station_ids vector of strings; Synoptic IDs of weather stations
#' @param date_range vector of Dates; min and max dates to fetch weather data
#' @param tzone_out the timezone in which all of the dates in the database will
#' be read in. Dates in duckdb appear to all be in UTC. Setting tzone_out to
#' tz seems to be equivalent to using lubridate::with_tz(time, tz).
#'
#' @return a nested tibble with station metadata on the top level and weather
#' data inside of "obs"
#'
#' @details
#' Only a single, unified timezone is supported for now, due to how the duckdb
#' was written.
#'
#' @export
fetch_weather_synoptic <- function(station_ids, date_range,
                                   tzone_out = Sys.timezone()) {
  processed_synoptic_path <- file.path(
    rappdirs::user_data_dir(appname = "weatherAndClimateDatabase"),
    "processed", "synoptic")

  # Connect to db
  synoptic_db <- DBI::dbConnect(drv = duckdb::duckdb(),
                        dbdir = file.path(processed_synoptic_path,
                                          "synoptic_flat.db"),
                        tzone_out = tzone_out)

  # Pre-process DB Query; The dplyr SQL interpreter does not like computations
  # inside the dplyr verbs
  date_range <- lubridate::force_tz(date_range, tz = tzone_out)
  min_date <- min(date_range); max_date <- max(date_range) + lubridate::days('1')
  station_ids <- toupper(station_ids)

  # Query DB for the right stations and times
  synoptic_subset_tbl <- dplyr::right_join(y = synoptic_db %>%
                                             dplyr::tbl("synoptic_weather"),
                                           x = synoptic_db %>%
                                             dplyr::tbl("synoptic_meta"),
                                           by = "Station_ID") %>%
    dplyr::filter(.data$Station_ID %in% station_ids,
                  .data$Date_Time_Local >= min_date,
                  .data$Date_Time_Local <= max_date) %>%
    dplyr::group_by(
      dplyr::across(tidyselect::all_of(
        c("Station_ID", "Station_Name", "latitude", "longitude", "elevation_ft",
          "state", "local_timezone")
      ))) %>%
    dplyr::collect() %>%
    dplyr::mutate(Date_Time_Local = lubridate::with_tz(.data$Date_Time_Local,
                                                       tzone_out)) %>%
    tidyr::nest(.key = "obs") %>%
    dplyr::ungroup()
  DBI::dbDisconnect(conn = synoptic_db)
  return(synoptic_subset_tbl)
}
