pacman::p_load("weatherAndClimateUtils", "weatherAndClimateDatabase")


# Description -------------------------------------------------------------


# Constants ---------------------------------------------------------------
station_equivalence_df <- data.frame(
  Station_ID = c("KSAN", "KMYF"),
  GHCN_ID = c("USW00023188", "USW00003131")
)

# Updated all GHCND, Synoptic, and unified daily DBs ----------------------
update_unified_db(station_equivalence_df)

# Update synoptic db ------------------------------------------------------
# First, synoptic csvs were added to:
# file.path(rappdirs::user_data_dir("weatherAndClimateDatabase"), "raw", "synoptic")"

# The DBs for synoptic weather and metadata are established from raw files
weatherAndClimateDatabase:::update_synoptic_db()


# Download GHCND records --------------------------------------------------
# GHCND csvs are downloaded to:
# file.path(rappdirs::user_data_dir("weatherAndClimateDatabase"), "raw", "ghcnd")
if(dl) {
  weatherAndClimateUtils:::get_data_ghcnd(dest_dir = file.path(
    rappdirs::user_data_dir("weatherAndClimateDatabase"), "raw", "ghcnd"),
    ids = station_equivalence_df$GHCN_ID)
}

# DBs for ghcnd weather and metadata are established from raw files
weatherAndClimateDatabase:::update_ghcnd_db()


# Relate stations ---------------------------------------------------------
# Metadata DB for synoptic weather is augmented with user-configured (for now)
# data.frame which relates synoptic and ghcnd IDs
processed_synoptic_path <- file.path(
  rappdirs::user_data_dir(appname = "weatherAndClimateDatabase"),
  "processed", "synoptic")

DBI::dbConnect(drv = duckdb::duckdb(),
               dbdir = file.path(processed_synoptic_path,
                                 "synoptic_meta.db")) %>%
  dplyr::tbl("synoptic_meta.db") %>%
  dplyr::collect() %>%
  dplyr::left_join(station_equivalence_df, by = "Station_ID")

weatherAndClimateDatabase:::relate_dbs(station_equivalence_df)
