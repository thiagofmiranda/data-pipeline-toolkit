#' @title Execute query on database.
#' @name query_db_mysql
#'
#' @param query_str Query to be executed.
#' @param database_host Connection host.
#'
#' @return Returns query results.
#'
#' @import DBI
#' @import RMySQL
#' @export
query_db_mysql <- function(query_str, database_host, user = NULL, password = NULL) {
  if (is.null(user)) user <- Sys.getenv("MYSQL_USER")
  if (is.null(password)) password <- Sys.getenv("MYSQL_PASS")
  DBI::dbGetQuery(create_conn_db(database_host, user, password), query_str)
}

#' @title Download daily applies to S3.
#' @name download_data
#'
#' @param daily_fun Daily data download function.
#' @param conn Database connection.
#' @param target_dates Vector of dates.
#' @param s3_path S3 storage location.
#' @param overwrite If overwrite=TRUE, overwrites old data.
#' @param parallel If parallel=TRUE, performs downloads in parallel.
#' @param verbose If verbose=TRUE, shows download status.
#'
#' @import dplyr
#' @import stringr
#' @import DBI
#' @import RMySQL
#' @import lubridate
#' @import furrr
#' @import arrow
#' @import future
#' @export
download_data <- function(daily_fun, conn, target_dates, s3_path, overwrite=F, parallel=F, verbose=T) {
  p <- progressor(along = target_dates)

  message(paste("Downloading:", str_squish(str_replace_all(str_remove_all(as.character(substitute(daily_fun)), "daily|download"), "_", " "))))

  save_data_query <- function(conn, query_date, s3_path) {
    p()
    daily_fun(conn, query_date) |>
      write_dataset_to_s3(s3_path)
  }

  if (!overwrite) {
    files_check <- check_files(dates = target_dates, s3_path = s3_path)
    target_dates <- target_dates[!files_check]
    message(paste(sum(files_check), "files already exist! downloading", sum(!files_check), "of", length(files_check)))
  }

  if (parallel) {
    plan(multisession, workers = PARALLEL_WORKERS)
  } else {
    plan(sequential)
  }

  future_pwalk(
    .l = list(
      conn = list(conn),
      s3_path = list(s3_path),
      query_date = target_dates
    ),
    .f = save_data_query,
    .options = furrr_options(seed = T, globals = T)
  )
}

#' @title Download daily applies to S3.
#' @name download_applies_daily
#'
#' @param conn Database connection.
#' @param query_date Download date.
#'
#' @import dplyr
#' @import stringr
#' @import DBI
#' @import RMySQL
#' @import lubridate
#' @export
download_applies_daily <- function(conn, query_date) {
  query <- paste0(
    "
    SELECT
      *
    FROM blabla env
    WHERE
      env.date >='", query_date, " 00:00:00' and
      env.date <='", query_date, " 23:59:59'
    ")
  df_applies <- query_db_mysql(query, database_host = conn, user = Sys.getenv("MYSQL_USER"), password = Sys.getenv("MYSQL_PASS")) |>
    mutate(apply_datetime = as_datetime(apply_datetime)) |>
    mutate(apply_date = as_date(apply_datetime)) |>
    mutate(year = year(apply_date)) |>
    mutate(month = month(apply_date)) |>
    mutate(day = day(apply_date))

  df_applies
}

download_data(
  daily_fun = download_applies_daily,
  s3_path = paste0(S3_PATH, "raw", "/", "applies", "/"),
  conn = get_secret("MYSQL_HOST"),
  target_dates = target_dates,
  overwrite = overwrite,
  parallel = parallel,
  verbose = verbose
)
