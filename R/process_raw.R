raw_synoptic_files_to_tibble <- function(raw_file_paths) {
  lapply(X = raw_file_paths,
         FUN = \(raw_file_path) {
           weatherAndClimateUtils::load_data_synoptic(raw_file_path)
         }) |>
    dplyr::bind_rows() |>
    dplyr::rename("Station_Name" = "STATION NAME") |>
    dplyr::rename_with(.cols = tidyselect::everything(),
                       .fn = \(x) {gsub(x = x, pattern = ' [ft]',
                                        replacement = '_ft',
                                        fixed = TRUE)}) |>
    dplyr::rename_with(.cols = -c("Station_Name", "Station_ID"),
                       .fn = tolower) |>
    dplyr::ungroup()
}


raw_ghcnd_files_to_tibble <- function(raw_file_paths) {
  lapply(X = raw_file_paths,
         FUN = \(raw_file_path) {
           readr::read_csv(raw_file_path, col_types = readr::cols()) %>%
             weatherAndClimatePlots::clean_raw_ghcnd_to_standard()
         }) |>
    dplyr::bind_rows() |>
    dplyr::rename("GHCN_ID" = "station") %>%
    dplyr::ungroup() %>%
    dplyr::group_by(dplyr::across(
      tidyselect::all_of(c("GHCN_ID",
                           "latitude", "longitude",
                           "elevation", "name"))
    )) %>%
    tidyr::nest(.key = "obs") %>%
    dplyr::ungroup()
}
