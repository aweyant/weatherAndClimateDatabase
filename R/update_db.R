update_ghcnd_db <- function() {
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
}

update_synoptic_db <- function() {
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
}

relate_dbs <- function(station_equivalence_df) {
  processed_unified_path <- file.path(
    rappdirs::user_data_dir(appname = "weatherAndClimateDatabase"),
    "processed", "unified")
  processed_synoptic_path <- file.path(
    rappdirs::user_data_dir(appname = "weatherAndClimateDatabase"),
    "processed", "synoptic")

  # Create dir for db if it does not exist
  if (!dir.exists(processed_unified_path)) {dir.create(processed_unified_path)}

  # Open connections to Synoptic metadata and establish a unified metadata db
  con_synoptic <- DBI::dbConnect(drv = duckdb::duckdb(),
                                 dbdir = file.path(processed_synoptic_path,
                                                        "synoptic_flat.db"))
  con_unified <- DBI::dbConnect(drv = duckdb::duckdb(),
                                dbdir = file.path(processed_unified_path,
                                                  "unified.db"))

  # Join the Synoptic metadata to the matching GHCN IDs, whenever they exist
  unified_meta_tbl <- con_synoptic %>%
    dplyr::tbl("synoptic_meta") %>%
    dplyr::collect() %>%
    dplyr::left_join(station_equivalence_df, by = "Station_ID")


  # Write weather data to db
  DBI::dbWriteTable(con_unified, "unified_meta",
                    value = unified_meta_tbl)

  # Close db connections
  duckdb::dbDisconnect(conn = con_synoptic)
  duckdb::dbDisconnect(conn = con_unified)
}
