if(!require(pacman)){install.packages("pacman")};library(pacman)
#library(sotm)

p_load(aips,arrow,aws.s3,DBI,dplyr,dtplyr,furrr,future,getPass,ggh4x,ggnewscale,
ggplot2,ggpubr,ggtext,httr,jsonlite,lubridate,magick,progressr,purrr,bizdays,forcats,
readr,RMySQL,robustbase,scales,SnowballC,stringi,stringr,tibble,tidyjson,tidyr,tm)

source("R/download_functions.R")
source("R/enrich_functions.R")
source("R/merge_functions.R")
source("R/stats_plot_functions.R")
source("R/miro.R")
source("R/utils.R")

# Configurações iniciais
options(future.globals.maxSize= 7000*1024^2)
options(cli.progress_handlers = "progressr")
handlers(handler_pbcol(
  adjust = 0,
  complete = function(s) cli::bg_red(cli::col_black(s)),
  incomplete = function(s) cli::bg_cyan(cli::col_black(s))
))
options(warn=-1)
options(dplyr.summarise.inform = FALSE)

# Objetos utilizados ao longo da geração do report
colors <- tribble(
  ~category,                 ~color,
  # Dias
  "Fim de Semana"          ,"#F6AA54",
  "Dia Útil"               ,"#23989B",
  "Feriado"                ,"#E06988")