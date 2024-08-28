station_equivalence_df <- data.frame(
  Station_ID = c("KSAN", "KMYF"),
  GHCN_ID = c("USW00023188", "USW00003131")
)

update_unified_db(station_equivalence_df)
