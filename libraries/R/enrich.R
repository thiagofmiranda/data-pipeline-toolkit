#' @title Baixa applies por dia para o S3.
#' @name enrich_data
#'
#' @param daily_fun Função de download de dados diário.
#' @param conn Conexão com o banco de dados.
#' @param target_dates Vetor de datas.
#' @param s3_path Local de armazenamento no S3.
#' @param overwrite Se overwrite=TRUE, sobrescreve os dados antigos.
#' @param parallel Se parallel=TRUE, realiza os downloads paralelamente.
#' @param verbose Se verbose=TRUE, Mostra os status dos downloads
#'
#' @author Thiago Miranda
#'
#'
#' @import progressr
#' @import dplyr
#' @import stringr
#' @import DBI
#' @import RMySQL
#' @import lubridate
#' @import furrr
#' @import arrow
#' @import future
#' @export
enrich_data <- function(daily_fun, conn, target_dates,s3_path, overwrite=F,parallel=F,verbose=T){
  p <- progressor(along = target_dates)

  message(paste("Enriching:", str_squish(str_replace_all(str_remove_all(as.character(substitute(daily_fun)),"daily|enrich"),"_"," "))))

  if(!overwrite){
    files_check <- check_files(dates = target_dates,s3_path = s3_path)
    target_dates <- target_dates[!files_check]
    message(paste(sum(files_check),"files already exist! downloading",sum(!files_check),"of",length(files_check)))
    message(paste(target_dates,collapse = ","))
  }

  # add datasets to joins 


  save_data_query <- function(conn,query_date,s3_path) {
    p()

    res <- daily_fun(conn,query_date) |>
      write_dataset_to_s3(s3_path)

    res
  }

  if(parallel){
    plan(multisession, workers = PARALLEL_WORKERS)
  }else{
    plan(sequential)
  }


  future_pwalk(
    .l=list(
      conn=list(conn),
      s3_path=list(s3_path),
      query_date = target_dates
    ),
    .f = save_data_query,
    .options = furrr_options(seed = T,globals = T)
  )

}


#' @title Enriquece os dados de applies do S3.
#' @name enrich_applies_daily
#'
#' @param conn Conexão com o banco de dados.
#' @param query_date Data.
#'
#' @author Thiago Miranda
#'
#'
#' @import dplyr
#' @import stringr
#' @import DBI
#' @import RMySQL
#' @import lubridate
#' @import furrr
#' @import arrow
#' @import dtplyr
#' @import future
#' @import SnowballC
#' @export
enrich_applies_daily <- function(conn, query_date){

  data_model <- data.frame(ID_ENVO_CRRO=integer(),
                           apply_datetime=POSIXct(),
                           usr_id=integer(),
                           vag_id=integer(),
                           cur_id=integer(),
                           ID_ORGM_ENVO_CRRO=integer(),
                           user_plan=character(),
                           user_plan_price=double(),
                           ID_DSPO_ENVO_CRRO=integer(),
                           score=double(),
                           apply_date=Date(),###
                           year=integer(),
                           month=integer(),
                           day=integer(),
                           report_channel=character(),
                           apply_device_channel=character(),
                           subscription=character(),
                           stringsAsFactors=FALSE)

  df_applies <- read_dataset_by_date(query_date,s3_path=paste0(S3_PATH,"raw/applies/"),date_var = "apply_date")

  if(nrow(df_applies)==0){
    left_join(df_applies,data_model) |>
      suppressMessages()
  }else{
    df_applies <- df_applies |>
      left_join(df_applies_type, by = "ID_ORGM_ENVO_CRRO" ) |>
      left_join(df_applies_dispositos, by = c("ID_ORGM_ENVO_CRRO","ID_DSPO_ENVO_CRRO"))

    df_applies <- df_applies |>
      mutate(apply_date = as_date(apply_date)) |>
      mutate(subscription = categorize_plans(user_plan,user_plan_price))
    df_applies
  }

}

enrich_data(
      daily_fun = enrich_applies_daily,
      s3_path = paste0(S3_PATH,"enriched/applies/"),
      conn = get_secret("MYSQL_HOST"),
      target_dates = target_dates,
      overwrite = overwrite,
      parallel = parallel,
      verbose = verbose
    )