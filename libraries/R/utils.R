#' stats_test
#'
#' Test dataframe with simulated data.
#'
#' @name stats_test
#' @keywords test
#' @docType data
#' @details
#'     Test dataframe with simulated data.
#'
"stats_test"

#' colors
#'
#' Dataframe with standardized colors by category.
#'
#' @name colors
#' @keywords colors
#' @docType data
#' @details
#'     Dataframe with standardized colors by category.
#'
"colors"

#' @title Format number to SI text
#' @name si_scale
#'
#' @description A function that formats number to SI.
#'
#' @param x Vector of numbers.
#'
#'
#' @return Vector of numbers in SI (character).
#'
#' @author Thiago Miranda
#'
#'
#' @examples
#' si_scale(10^6)
#'
#' @import stringr
#' @import scales
#' @export
si_scale <- function(x) {
  fun <- scales::label_number(accuracy = 0.1, scale_cut = scales::cut_si(unit = ""))
  fun(x) |>
    stringr::str_remove_all("\\s") |>
    stringr::str_to_upper()
}

#' @title Format number to SI text
#' @name si_scale2
#'
#' @description A function that formats number to SI.
#'
#' @param x Vector of numbers.
#'
#'
#' @return Vector of numbers in SI (character).
#'
#' @author Thiago Miranda
#'
#'
#' @examples
#' si_scale2(10^6)
#'
#' @import stringr
#' @import scales
#' @export
si_scale2 <- function(x) {
  fun <- scales::label_number(accuracy = 1, scale_cut = scales::cut_si(unit = ""))
  fun(x) |>
    stringr::str_remove_all("\\s") |>
    stringr::str_to_upper()
}

#' @name pt_month
#'
#' @description A function that converts date to month in pt_BR.
#'
#' @param date Vector of dates.
#' @param label TRUE/FALSE value to return the names of the months.
#' @param abbr TRUE/FALSE value to return the abbreviated names of the months.
#'
#'
#' @return Vector of months in pt_BR.
#'
#' @author Thiago Miranda
#'
#'
#' @examples
#' pt_month("2022-01-01")
#'
#' @import stringr
#' @import lubridate
#' @export
pt_month <- function(date, label = TRUE, abbr = TRUE) {
  en_abb <- c("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
  pt_abb <- c("Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez")
  en_name <- c("January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December")
  pt_name <- c("Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro")
  if (abbr) {
    levels <- en_abb
    labels <- pt_abb
  } else {
    levels <- en_name
    labels <- pt_name
  }
  month <- date |>
    lubridate::as_date() |>
    lubridate::month(label = label, abbr = abbr, locale = "en_US.UTF-8")
  if (label) {
    month <- month |>
      stringr::str_to_sentence() |>
      factor(levels, labels, ordered = TRUE)
  }
  return(month)
}

#' @description A function that generates a list of predefined colors for plot construction.
#'
#' @param labels Vector of categories to extract colors.
#' @param df_colors Dataframe of colors, columns: ~category, ~color.
#'
#'
#' @return A list of colors for the defined categories.
#'
#' @author Thiago Miranda
#'
#'
#' @examples
#' get_colors("Auto-Apply")
#'
#' @import dplyr
#' @import tibble
#' @export
get_colors <- function(labels, df_colors = colors) {
  selected_colors <- df_colors |>
    dplyr::filter(category %in% labels) |>
    dplyr::mutate(category = factor(category, levels = labels)) |>
    dplyr::arrange(category)
  colors_cat <- selected_colors$color
  names(colors_cat) <- selected_colors$category
  return(colors_cat)
}

#' @description A function that creates a connection to the database.
#'
#' @param database_host String with the database host.
#' @param user String with the database user.
#' @param password String with the database password.
#'
#' @import DBI
#' @import RMySQL
#' @export
create_conn_db <- function(database_host, user, password) {
  # creating connection to the database
  lapply(dbListConnections(dbDriver(drv = "MySQL")), dbDisconnect)
  cn <- dbConnect(MySQL(), user = user, password = password, host = database_host)
  return(cn)
}

#' @title Ask user for database connection credentials
#' @name ask_secrets
#'
#' @author Thiago Miranda
#'
#'
#' @import getPass
#' @export
ask_secrets <- function() {
  secret_str <- getPass::getPass("Please inform Database Credentials")
  eval(
    parse(
      text = paste0(
                    "Sys.setenv(", 
                    paste(as.character(parse(text = secret_str)), collapse = ","),
                    ")"
         )
        ), 
    envir = .GlobalEnv)
}

#' @title Ask user for Miro connection credentials.
#' @name askMiroToken
#'
#' @author Thiago Miranda
#'
#' @examples
#' # askMiroToken()
#'
#' @import getPass
#' @export
ask_secrets <- function() {
  secret_str <- getPass::getPass("Please inform Database Credentials")
  eval(parse(text = paste0("Sys.setenv(", paste(as.character(parse(text = secret_str)), collapse = ","), ")")), envir = .GlobalEnv)
}

#' @title Get environment variable for database connection.
#' @name get_secret
#'
#' @param name Name of the environment variable.
#'
#' @return Returns the value of the environment variable.
#'
#' @author Thiago Miranda
#'
#' @export
get_secret <- function(name) {
  Sys.getenv(name)
}


#' @title Convert encoding of an object to latin1 encoding.
#' @name to_latin1
#'
#' @param x Object.
#'
#' @return Returns the value of the variable with latin1 encoding.
#'
#' @author Thiago Miranda
#'
to_latin1 <- function(x) {
  Encoding(x) <- "latin1"
  x
}

#' @title Create a date vector from a range.
#' @name date_range
#'
#' @param start_date Start date.
#' @param end_date End date.
#'
#' @return Returns a date vector from a range.
#'
#' @author Thiago Miranda
#'
#' @examples
#' date_range("2022-01-01", "2022-01-31")
#'
#' @import lubridate
#' @export
date_range <- function(start_date, end_date) {
  seq(from = lubridate::as_date(start_date), to = lubridate::as_date(end_date), by = "days")
}

#' @title Convert a date to its month value in pt-br.
#' @name month_br
#'
#' @param date Date.
#'
#' @return Returns a date to its month value in pt-br.
#'
#' @author Thiago Miranda
#'
#' @examples
#' month_br("2022-01-01")
#' @export
month_br <- function(date) {
  date <- lubridate::as_date(date)
  month <- readr::locale(date_names = "pt")$date_names$mon_ab[as.numeric(stringr::str_sub(date, 6, 7))]
  date_sort <- sort(lubridate::as_date(date), decreasing = FALSE)
  month_sort <- unique(readr::locale(date_names = "pt")$date_names$mon_ab[as.numeric(stringr::str_sub(date_sort, 6, 7))])
  factor(month, month_sort)
}

#' @title Check if a file from a date exists in an S3 directory.
#' @name check_files
#'
#' @import purrr
#' @import aws.s3
#' @import stringr
#' @export
check_files <- function(dates, s3_path) {
  prefix <- stringr::str_extract(s3_path, "(?<=\\w{1}\\/).*")
  bucket <- stringr::str_remove(s3_path, "(?<=\\w{1}\\/).*")
  check <- aws.s3::get_bucket_df(bucket = bucket, prefix = prefix, max = Inf) |>
    dplyr::filter(stringr::str_detect(Key, "day")) |>
    dplyr::mutate(date = lubridate::as_date(paste0(stringr::str_extract(Key, "(?<=year=).*(?=\\/month)"), "-", stringr::str_extract(Key, "(?<=month=).*(?=\\/day)"), "-", stringr::str_extract(Key, "(?<=day=).*(?=\\/)")))) |>
    dplyr::pull(date)
  return(dates %in% check)
}

#' @title Check which files do not exist in an S3 directory.
#' @name check_files
#'
#' @param type_level String with the data type level to be used, "raw", "enriched" or "merged".
#' @param type String with the data type to be used. Ex: "applies".
#'
#' @return Returns a date vector indicating which files do not exist in S3.
#'
#' @author Thiago Miranda
#'
#' @import lubridate
#' @import furrr
#' @import purrr
#' @import aws.s3
#' @export
get_missing_files <- function(target_dates, type_level, type) {
  check <- check_files(dates = target_dates, s3_path = paste0(S3_PATH, type_level, "/", type, "/"))
  cat(paste0(length(target_dates[!check]), " missing file(s)."))
  return(target_dates[!check])
}

#' @title Read files given a date vector.
#' @name read_dataset_by_date
#'
#' @param target_dates Date vector.
#' @param s3_path Location in S3.
#' @param date_var Name of the date variable in the file.
#'
#' @return Returns a file with data from all date files.
#'
#' @author Thiago Miranda
#'
#' @examples
#' # read_dataset_by_date(
#' @import arrow
#' @import dplyr
#' @export
read_dataset_by_date <- function(target_dates, s3_path, date_var) {
  date <- lubridate::as_date(target_dates)
  f_year <- unique(lubridate::year(target_dates))
  f_month <- unique(lubridate::month(target_dates))
  f_day <- unique(lubridate::day(target_dates))
  if (length(target_dates) == 1) {
    arrow::open_dataset(s3_path) |>
      dplyr::filter(year %in% f_year, month %in% f_month, day %in% f_day) |>
      dplyr::collect()
  } else {
    arrow::open_dataset(s3_path) |>
      dplyr::filter(year %in% f_year, month %in% f_month, day %in% f_day) |>
      dplyr::filter(!!rlang::sym(date_var) %in% target_dates) |>
      dplyr::collect()
  }
}

#' @title Normalize text.
#' @name normalizer
#'
#' @param term Text to be normalized.
#' @param language Language of the text.
#' @param stemming Boolean indicating whether to apply stemming.
#'
#' @return Returns the normalized text.
#'
#' @author Thiago Miranda
#'
#' @examples
#' # normalizer("Texto a ser normalizado", "pt", TRUE)
#'
#' @import tm
#' @import stringi
#' @export
normalizer <- function(term, language, stemming = FALSE) {
  options(warn = -1)
  stopwords_pt <- tm::stopwords("pt") |>
    stringi::stri_trans_general(id = "Latin-ASCII")
  remove_accents <- tm::content_transformer(function(x) stringi::stri_trans_general(x, id = "Latin-ASCII"))
  mycorpus <- term |>
    tm::VectorSource() |>
    tm::Corpus() |>
    tm::tm_map(remove_accents) |>
    tm::tm_map(tm::content_transformer(tolower)) |>
    tm::tm_map(tm::removePunctuation) |>
    tm::tm_map(tm::removeNumbers) |>
    tm::tm_map(tm::removeWords, stopwords_pt) |>
    tm::tm_map(tm::stripWhitespace) |>
    tm::tm_map(tm::content_transformer(stringr::str_squish))
  if (stemming) {
    result <- sapply(mycorpus, identity) |>
      tm::stemDocument(language = language)
  } else {
    result <- sapply(mycorpus, identity)
  }
  options(warn = 0)
  return(result)
}

#' @title Write dataset to S3.
#' @name write_dataset_to_s3
#'
#' @param data Data to be written.
#' @param s3_path Location in S3.
#'
#' @author Thiago Miranda
#'
#' @examples
#' # write_dataset_to_s3(data, s3_path)
#'
#' @import arrow
#' @export
write_dataset_to_s3 <- function(data, s3_path) {
  arrow::write_dataset(
    dataset = data,
    path = s3_path,
    format = c("parquet"),
    partitioning = c("year", "month", "day"),
    existing_data_behavior = "overwrite"
  )
}

#' @title Generate dates for report.
#' @name report_dates
#'
#' @param year Year of the report.
#' @param month Month of the report.
#' @param MoM Month-over-Month comparison.
#' @param YoY Year-over-Year comparison.
#'
#' @return Returns a vector of dates for the report.
#'
#' @author Thiago Miranda
#'
#' @examples
#' # report_dates(year = 2023, month = 8, MoM = 0, YoY = 0)
#'
#' @import lubridate
#' @import purrr
#' @export
report_dates <- function(year, month, MoM = 0, YoY = 0) {
  ref_start <- lubridate::as_date(paste0(year, "-", month, "-01"))
  ref_end <- lubridate::as_date(paste0(year, "-", month, "-01")) + months(1) - lubridate::days(1)
  ref_mom <- date_range(ref_start - months(MoM), ref_end)
  unique(sort(lubridate::as_date(unlist(purrr::map(.x = 0:YoY, .f = function(x) { ref_mom - lubridate::years(x) })))))
}
