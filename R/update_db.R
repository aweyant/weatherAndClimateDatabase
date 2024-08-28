update_ghcnd_db <- function(station_equivalence_df = NULL) {
  # Specify raw and processed data paths
  raw_ghcnd_path <- file.path(
    rappdirs::user_data_dir(appname = "weatherAndClimateDatabase"),
    "raw", "ghcnd")
  processed_ghcnd_path <- file.path(
    rappdirs::user_data_dir(appname = "weatherAndClimateDatabase"),
    "processed", "ghcnd")

  # Create dir for db if it does not exist
  if (!dir.exists(processed_ghcnd_path)) {dir.create(processed_ghcnd_path)}

  # Open connections to ghcnd weather and metadata dbs
  con <- DBI::dbConnect(drv = duckdb::duckdb(),
                        dbdir = file.path(processed_ghcnd_path,
                                          "ghcnd_flat.db"))

  # Create tibbles with ghcnd weather data and station-level metadata
  ghcnd_tbl <- raw_ghcnd_files_to_tibble(
    raw_file_paths = list.files(path = raw_ghcnd_path,
                                pattern = "*.csv",
                                full.names = TRUE)
  )

  # Write weather data to db
  DBI::dbWriteTable(con, "ghcnd_weather",
                    value = ghcnd_tbl %>%
                      dplyr::select(.data$GHCN_ID, .data$obs) %>%
                      tidyr::unnest("obs"))
  # Write station metadata to db
  DBI::dbWriteTable(con, "ghcnd_meta",
                    ghcnd_tbl %>%
                      dplyr::select(-c("obs")))
  # Close db connections
  duckdb::dbDisconnect(conn = con)

  # When called from update_unified_df(), a station_equivalence_df is provided
  # and in this case, we return the subset of the ghcnd_tbl mentioned in this
  # equivalence table.
  if(!is.null(station_equivalence_df)) {
    return(ghcnd_tbl %>%
             dplyr::filter(.data$GHCN_ID %in%
                             unique(station_equivalence_df$GHCN_ID)))
  }
}

update_synoptic_db <- function(station_equivalence_df) {
  # Specify raw and processed data paths
  raw_synoptic_path <- file.path(
    rappdirs::user_data_dir(appname = "weatherAndClimateDatabase"),
    "raw", "synoptic")
  processed_synoptic_path <- file.path(
    rappdirs::user_data_dir(appname = "weatherAndClimateDatabase"),
    "processed", "synoptic")

  # Create dir for db if it does not exist
  if (!dir.exists(processed_synoptic_path)) {dir.create(processed_synoptic_path)}

  # Open connections to synoptic weather
  con <- DBI::dbConnect(drv = duckdb::duckdb(),
                        dbdir = file.path(processed_synoptic_path,
                                          "synoptic_flat.db"))

  # Create tibbles with synoptic weather data and station-level metadata
  synoptic_tbl <- raw_synoptic_files_to_tibble(
    raw_file_paths = list.files(path = raw_synoptic_path,
                                pattern = "*.csv",
                                full.names = TRUE)
  )

  # Write weather data to db
  DBI::dbWriteTable(con, "synoptic_weather",
                    value = synoptic_tbl %>%
                      dplyr::select(.data$Station_ID, .data$obs) %>%
                      tidyr::unnest("obs"))
  # Write station metadata to db
  DBI::dbWriteTable(con, "synoptic_meta",
                    synoptic_tbl %>%
                      dplyr::select(-c("obs")))
  # Close db connections
  duckdb::dbDisconnect(conn = con)

  # When called from update_unified_df(), a station_equivalence_df is provided
  # and in this case, we return the subset of the ghcnd_tbl mentioned in this
  # equivalence table.
  if(!is.null(station_equivalence_df)) {
    return(synoptic_tbl)
  }
}

update_unified_db <- function(station_equivalence_df) {
  # Check that there is a raw data directory
  raw_dir <- file.path(
    rappdirs::user_data_dir(appname = "weatherAndClimateDatabase"),
    "raw")
  if(!dir.exists(raw_dir)) {
    dir.create(raw_dir)
    print(paste0(
      "You must fill a directory called ", raw_dir,
      " and fill subdirectories ghcnd/ and synoptic/ files for each weather station."))
    return(NULL)}
  # Create a processed data directory, if it does not exist
  processed_dir <- file.path(
    rappdirs::user_data_dir(appname = "weatherAndClimateDatabase"),
    "processed")
  if (!dir.exists(processed_dir)) {dir.create(processed_dir)}


  # Establish or connect to existing unified db
  processed_unified_path <- file.path(
    rappdirs::user_data_dir(appname = "weatherAndClimateDatabase"),
    "processed", "unified")

  # Create dir for db if it does not exist
  if (!dir.exists(processed_unified_path)) {dir.create(processed_unified_path)}

  con_unified <- DBI::dbConnect(drv = duckdb::duckdb(),
                                dbdir = file.path(processed_unified_path,
                                                  "unified.db"))

  # Get GHCN and Synoptic up-to-date, and make use of the calculations performed
  # so far.
  # GHCND obs and an open database connection in a list
  print("Updating GHCN DB")
  ghcnd_tbl <- update_ghcnd_db(station_equivalence_df)
  # Synoptic obs and an open database connection in a list
  print("Updating Synoptic DB")
  synoptic_tbl <- update_synoptic_db(station_equivalence_df)

  print("Aggregating Synoptic to daily res and merging with GHCND")
  synoptic_tbl %>%
    dplyr::full_join(
      y = tibble::as_tibble(station_equivalence_df) %>%
        dplyr::full_join(ghcnd_tbl, by = "GHCN_ID"),
      by = "Station_ID") %>%
    dplyr::mutate(
      latitude = purrr::pmap_dbl(
        .l = list(.data$latitude.x, .data$latitude.y),
        .f = \(a,b) {min(c(a,b), na.rm = TRUE)}),
      longitude = purrr::pmap_dbl(
        .l = list(.data$longitude.x, .data$longitude.y),
        .f = \(a,b) {min(c(a,b), na.rm = TRUE)}),
      .keep = "unused") %>%
    # Summarization of Synoptic data to daily res and merging with GHCND
    dplyr::mutate(obs_daily = purrr::pmap(
      .l = list(.data$Station_ID, .data$obs.x, .data$obs.y),
      .f = \(station_id, df_synoptic, df_ghcnd) {
        # GHCND are the daily obs if there are no Synoptic obs.
        # weatherAndClimateUtils::merge_data() already accounts for this case,
        # but this spares us another function call.
        if(is.null(df_synoptic)) return(df_ghcnd)
        # Otherwise, daily obs are a combination of aggregated Synoptic obs and
        # GHCND obs
        df_synoptic %>%
          # Aggregate Synoptic weather to daily resolution.
          # Notably, Date_Time_Local should always be correct, because
          # aggregate_data() is being called on the tibble derived from raw
          # Synoptic files rather than DB files.
          weatherAndClimateUtils::aggregate_data() %>%
          # Merge summarized Synoptic with GHCND
          {
            if(is.null(df_ghcnd)) {(.)}
            else {
              weatherAndClimateUtils::merge_data(
                synoptic_daily_df = .,
                ghcnd_df = df_ghcnd %>%
                  dplyr::select(
                    tidyselect::any_of(c("date", "tmin","tmax","precip"))
                  ))}
          }
      })) %>%
    {
      print("Writing unified daily DB.")
      # Write weather data to db
      DBI::dbWriteTable(con_unified, "unified_daily_weather",
                        value = (.) %>%
                          dplyr::select(.data$Station_ID, .data$obs_daily) %>%
                          tidyr::unnest("obs_daily"))
      # Write station metadata to db
      DBI::dbWriteTable(con_unified, "unified_meta",
                        (.) %>%
                          dplyr::select(-c("obs.x", "obs.y", "obs_daily")))
    }
}
