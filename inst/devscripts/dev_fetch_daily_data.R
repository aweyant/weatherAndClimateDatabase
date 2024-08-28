station_ids = c("ksan", "g3667")
date_range = lubridate::ymd(c("2022-03-01", "2024-04-21"))

fetch_weather_daily(station_ids, date_range)
