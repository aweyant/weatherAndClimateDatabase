# ARGS --------------------------------------------------------------------
station_ids = "g3667"
date_range = lubridate::ymd(c("2025-01-01", "2025-01-08"))
tzone_out = Sys.timezone()


# CODE --------------------------------------------------------------------
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

# NEAR-FUTURE UPDATE TO SUPPORT AUTOMATED DB
# if stations do not have data for daterange and the last update was a long
# time ago, start the automatic updating of the db
# Do so by calling a function with station_ids, date_range, and tzone out
synoptic_db %>%
  dplyr::tbl("synoptic_meta") %>%
  dplyr::filter(.data$Station_ID %in% station_ids) %>%
  dplyr::mutate()

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
  dplyr::arrange(dplyr::desc(.data$Date_Time_Local)) %>%
  dplyr::collect() %>%
  dplyr::mutate(Date_Time_Local = lubridate::with_tz(.data$Date_Time_Local,
                                                     tzone_out)) %>%
  tidyr::nest(.key = "obs") %>%
  dplyr::ungroup()
DBI::dbDisconnect(conn = synoptic_db)
