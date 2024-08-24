test_that("For reasonable query: output is a tibble", {
  station_ids = c("ksan", "KMYF", "g3667")
  date_range = lubridate::ymd(c("2024-08-01", "2024-08-05"))
  synoptic_subset_tbl <- fetch_weather_synoptic(station_ids, date_range)
  expect_equal("tbl" %in% class(synoptic_subset_tbl), TRUE)
})
