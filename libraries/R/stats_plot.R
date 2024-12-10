#' @title Gera descritivas com base nos dados do S3.
#' @name summarise_data
#'
#' @param target_dates Vetor de datas.
#' @param stat_id String indicando a coluna que será contada com distinção.
#' @param filter_var String com a coluna a ser filtrada. (Opcional)
#' @param filter_values Vetor com os valores a serem filtrados. (Opcional)
#' @param type String com o tipo do dado a ser utilizado.
#' @param type_level String com o nível do tipo do dado a ser utilizado, "raw", "enriched" ou "merged".
#' @param groups Vetor com as colunas a serem agrupadas.
#'
#' @return Retorna o data.frame com as estatísticas.
#'
#' @author Thiago Miranda
#'
#' @import lubridate
#' @import arrow
#' @import dplyr
#' @import stringr
#'
#' @export
summarise_data <- function(target_dates,stat_id="usr_id",filter_var="",filter_values,df_join=NULL,df_join_by=NULL,type="job-organic-logged-searches",type_level="enriched", groups=c("year","month","day","keywords_norm","keywords_stem","query_classification","subscription","fl_paid_user","query_classification_tags","open_query")){

  dates <- case_when(
    type=="job-organic-logged-searches" ~ "search_date",
    type=="cv-organic-logged-searches" ~ "search_date",
    type=="applies" ~ "apply_date",
    type=="contacts" ~ "contact_date",
    type=="contacts-applies" ~ "contact_date",
    type=="job-postings" ~ "job_posting_date",
    type=="active-jobs" ~ "job_active_date",
    type=="apply-attempts" ~ "apply_attempt_date",
    type=="logs-health-check" ~ "log_date",
    type=="standout" ~ "standout_date"
  )
  stats_name <- case_when(
    type=="job-organic-logged-searches" ~ "searches",
    type=="cv-organic-logged-searches" ~ "searches",
    type=="applies" ~ "applies",
    type=="contacts" ~ "contacts",
    type=="contacts-applies" ~ "contacts",
    type=="job-postings" ~ "job_postings",
    type=="active-jobs" ~ "active_jobs",
    type=="apply-attempts" ~ "apply_attempts",
    type=="logs-health-check" ~ "status_hits",
    type=="standout" ~ "standouts"
  )

  s3_path <- paste0(S3_PATH,type_level,"/",type,"/")

  target_dates <- as_date(target_dates)

  f_year <- unique(year(target_dates))
  f_month <- unique(month(target_dates))
  f_day <- unique(day(target_dates))

  ds <- open_dataset(s3_path) |>
    filter(year %in% f_year, month %in% f_month, day %in% f_day) |>
    filter(!!sym(dates) %in% target_dates)

  contacts_types <<- "Aba Contato - Deslogado|Agendamento de entrevista|Convite para vaga|Envio de mensagem|Impressão de curriculo|Marcação|Movimentação de Etapa|Visualização dos dados de contato"

  if(type=="job-organic-logged-searches"){
    ds <- ds |>
      filter(is_pja==0)
  }else if(type=="applies"){
    ds <- ds |>
      mutate(apply_quali = case_when(score>=0.75 ~ "Apply Altamente Qualificado",
                                     score>=0.65 ~ "Apply Minimamente Qualificado",
                                     score < 0.65 ~ "Apply Não-Qualificado",
                                     TRUE ~ NA_character_))
  }else if(type=="contacts"){
    ds <- ds |>
      filter(!emp_id %in% contact_emps_blacklist) |>
      filter(str_detect(descricao,contacts_types))
  }

  if(filter_var!=""){
    ds <- ds |>
      filter(!!sym(filter_var) %in% filter_values)
  }

  if(!is.null(df_join)){
    ds <- ds |>
      left_join(df_join, by=df_join_by)
  }

  if(type=="logs-health-check"){
    ds <- ds |>
      select_at(c(groups,"status_hit")) |>
      group_by(across(all_of(groups))) |>
      summarise(!!sym(stats_name) := sum(status_hit)) |>
      ungroup() |>
      collect()
  }else{
    ds <- ds |>
      select_at(c(groups,stat_id)) |>
      group_by(across(all_of(groups))) |>
      summarise(!!sym(stats_name) := n(),ids = n_distinct(!!sym(stat_id))) |>
      mutate(!!sym(paste0(stats_name,"_per_ids")) := !!sym(stats_name)/ids) |>
      ungroup() |>
      collect()
  }
  if("day" %in% groups){
    #check missing days
    day_in <- unique(as_date(paste0(ds$year,"-",ds$month,"-",ds$day)))
    day_out <- target_dates[!target_dates %in% day_in]

    if(length(day_out)>0){
      ds_empty <- data.frame(year=year(day_out),month=month(day_out),day=day(day_out))

      ds_out <- ds |>
        select_at(groups) |>
        select(-year,-month,-day)  |>
        unique() |>
        expand_grid(ds_empty) |>
        mutate(!!sym(stats_name) := 0, ids = 0, !!sym(paste0(stats_name,"_per_ids")) := 0) |>
        ungroup()

      ds <- bind_rows(ds,ds_out)
    }
    ds <- ds |>
      mutate(day = str_pad(day,2,"left","0")) |>
      mutate(day_type = case_when(
        wday(as_date(paste0(year,"-",month,"-",day))) %in% c(1,7) ~ "Fim de Semana",
        as_date(paste0(year,"-",month,"-",day))  %in% br_bizdays ~ "Feriado",
        TRUE ~ "Dia Útil"))
  }
  return(ds)
}

#' @title Cria estatísticas para geração de gráficos com base nas descritivas.
#' @name generate_stats
#'
#' @param df Data.frame com as descritivas para plot.
#' @param stats_var String com a variável da estatística.
#' @param category_var String com a variável de label.
#' @param groups Vetor com as colunas a serem agrupadas juntamente com category_var.
#' @param n_decimals Número de casas decimais a serem consideradas.
#'
#' @return Retorna o data.frame com as estatísticas.
#'
#' @author Thiago Miranda
#'
#' @import lubridate
#' @import dplyr
#' @import stringr
#' @export
generate_stats <- function(df,stats_var, category_var=NULL, groups=NULL,n_decimals=1){

  if(n_decimals==0){
    scales_fun <- si_scale2
  }else{
    scales_fun <- si_scale
  }

  if(!is.null(category_var)){

    if(is.null(groups)){
      df|>
        mutate(category = !!sym(category_var)) |>
        mutate(category = ifelse(is.na(category),"Indefinido",category)) |>
        group_by(category) |>
        summarise(stats = sum(!!sym(stats_var))) |>
        mutate(share = stats/sum(stats))|>
        mutate(share_label = paste0(round(share*100,n_decimals),"%")) |>
        mutate(stats_label = scales_fun(round(stats,n_decimals))) |>
        mutate(total = sum(stats)) |>
        mutate(total_label = scales_fun(round(total,n_decimals))) |>
        ungroup()

    }else{
      if("month" %in% groups){
        df <- df |>
          mutate(month = factor(as.numeric(month),levels=1:12,
                                labels = c("Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez")))
      }

      df|>
        mutate(category = !!sym(category_var)) |>
        mutate(category = ifelse(is.na(category),"Indefinido",category)) |>
        group_by(across(all_of(groups)),category) |>
        summarise(stats = sum(!!sym(stats_var))) |>
        group_by(across(all_of(groups))) |>
        mutate(share = stats/sum(stats))|>
        mutate(share_label = paste0(round(share*100,n_decimals),"%")) |>
        mutate(stats_label = scales_fun(round(stats,n_decimals))) |>
        mutate(total = sum(stats)) |>
        mutate(total_label = scales_fun(round(total,n_decimals))) |>
        ungroup()

    }

  }else{
    if("month" %in% groups){
      df <- df |>
        mutate(month = factor(as.numeric(month),levels=1:12,
                              labels = c("Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez")))
    }

    df |>
      group_by(across(all_of(groups))) |>
      summarise(stats = sum(!!sym(stats_var))) |>
      mutate(stats_label = scales_fun(round(stats,n_decimals))) |>
      ungroup()
  }

}

# ---------------------------------------------------- plot functions ----------------------------------------------------- #

#' @title Gera plot de share mensal
#' @name plot_monthly_share
#'
#'
#' @param df Dataframe de estatísticas formatado.
#' @param min.share.label Valor mínimo para a legenda ser exibida, default: 0.03.
#' @param legend.position posição da legenda ("top","bottom","left","right","none"), default: top.
#' @param legend.cols quantidade de colunas de legenda, default: 3.
#'
#' @return Plot de share
#'
#' @author Thiago Miranda
#'
#' @import dplyr
#' @import ggplot2
#' @import ggnewscale
#' @export
plot_monthly_share <- function (df,type=1, min.share.label=0.03, legend.position = "top", legend.cols = 3,title=NULL) {

  if(type == 1){
    colors <- get_colors(sort(unique(pull(df, category))))

    df <- df |>
      mutate(share_label = ifelse(share >=min.share.label, share_label, NA)) |>
      mutate(stats_label = ifelse(share >=min.share.label, stats_label, NA))

    ggplot(df, aes(x = month, y = share)) +
      geom_bar(mapping = aes(fill = category),
               width = 0.8, stat = "identity") +
      facet_grid(. ~ year, scales = "free", switch = "x", space = "free_x") +
      scale_fill_manual(values = colors, name = NULL, guide = guide_legend(ncol = legend.cols)) +
      new_scale_fill() +
      geom_label(aes(label = share_label, fill = category),
                 position = position_stack(vjust = 0.5), size = 4.5, color = "black", label.size = NA,
                 label.padding = unit(0.1,"lines"), label.r = unit(0, "lines")) +
      scale_fill_manual(values = rep("#FFFFFFC8",times = nrow(df))) +
      guides(fill = "none") +
      ylab("") +
      scale_y_continuous(limits = c(0, 1.02), expand = c(0,0)) +
      theme_light(base_size = 20) +
      ggtitle(title)+
      theme(panel.border = element_blank(),
            panel.grid = element_blank(), axis.line = element_line(linewidth = 0.5),
            axis.line.y = element_blank(), axis.title.x = element_blank(),
            axis.title.y = element_blank(), axis.text.y = element_blank(),
            axis.ticks = element_blank(), legend.justification = "center",
            plot.title = element_text(hjust = 0.5),
            legend.position = legend.position,
            legend.text = element_text(size = 12),strip.placement = "outside",
            strip.background = element_rect(color = "white",fill = "white", linewidth = 0.8, linetype = "solid"),
            strip.text = element_text(colour = "black", face = "bold"))

  }else if(type == 2){

    month_year <- function(sep="/"){
      dates <- seq(as_date("2000-01-01"),as_date("2099-01-01"),by="month")
      unique(
        paste0(
          factor(
            as.numeric(month(dates)),
            levels=1:12,
            labels = c("Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez")
          ),
          sep,
          str_sub(dates,3,4)
        )
      )

    }

    n_groups <- 2
    colors <- get_colors(sort(unique(pull(df, category))))

    df <- df |>
      mutate(share_label = ifelse(share >=min.share.label, share_label, NA)) |>
      mutate(stats_label = ifelse(share >=min.share.label, stats_label, NA))

    df |>
      mutate(group = factor(sort(rep(1:n_groups,n()/n_groups)),labels = c("YoY","Atual"),ordered = T)) |>
      mutate(month_year = factor(paste0(month,"/",str_sub(year,3,4)),levels = month_year())) |>
      ggplot(aes(x = month_year, y = share)) +
      geom_bar(mapping = aes(fill = category),
               width = 0.8, stat = "identity") +
      facet_grid(. ~ group, scales = "free", switch = "x", space = "free_x") +
      scale_fill_manual(values = colors, name = NULL, guide = guide_legend(ncol = legend.cols)) +
      new_scale_fill() +
      geom_label(aes(label = share_label, fill = category),
                 position = position_stack(vjust = 0.5), size = 4.5, color = "black", label.size = NA,
                 label.padding = unit(0.1,"lines"), label.r = unit(0, "lines")) +
      scale_fill_manual(values = rep("#FFFFFFC8",times = nrow(df))) +
      guides(fill = "none") +
      ylab("") +
      scale_y_continuous(limits = c(0, 1.02), expand = c(0,0)) +
      theme_light(base_size = 20) +
      ggtitle(title)+
      theme(panel.border = element_blank(),
            panel.grid = element_blank(), axis.line = element_line(linewidth = 0.5),
            axis.line.y = element_blank(), axis.title.x = element_blank(),
            axis.title.y = element_blank(), axis.text.y = element_blank(),
            axis.ticks = element_blank(), legend.justification = "center",
            plot.title = element_text(hjust = 0.5),
            legend.position = legend.position,
            legend.text = element_text(size = 12),strip.placement = "outside",
            strip.background = element_rect(color = "white",fill = "white", linewidth = 0.8, linetype = "solid"),
            strip.text = element_text(colour = "black", face = "bold"))

  }


}

#' @title Gera plot de volume mensal
#' @name plot_monthly_volume
#'
#'
#' @param df Dataframe de estatísticas formatado.
#' @param type Tipo de plot (1: plot com agrupamento, 2: plot simples), default:1.
#' @param min.share.label Valor mínimo para a legenda ser exibida, default: 0.03.
#' @param legend.position Posição da legenda ("top","bottom","left","right","none"), default: top.
#' @param legend.cols Quantidade de colunas de legenda, default: 3.
#' @param color Cor para o plot tipo 2, default: "#33AEB1".
#'
#' @return Plot de volume.
#'
#' @author Thiago Miranda
#'
#' @import dplyr
#' @import ggplot2
#' @import ggnewscale
#' @export
plot_monthly_volume <- function(df,type=1,min.share.label=0.03,legend.position="top",legend.cols=3,color="#33AEB1",title=NULL){


  if(type == 1){
    colors <- get_colors(sort(unique(pull(df,category))))

    df <- df |>
      mutate(share_label = ifelse(share >=min.share.label, share_label, NA)) |>
      mutate(stats_label = ifelse(share >=min.share.label, stats_label, NA))

    # Volume Graph
    df |>
      ggplot(aes(x=month,y=stats))+
      geom_bar(mapping = aes(fill=category),width = 0.8,stat = "identity") +
      facet_grid(.~year, scales = "free", switch = "x", space = "free_x")+
      #geom_line(aes(x=month, y=total,group=1),stat="identity",color="black",size=1.2)+
      geom_text(aes(x=month, y=total,group=1,label=total_label),stat="identity",vjust = -1,size=5,color="black")+
      scale_fill_manual(values=colors,name=NULL,guide = guide_legend(ncol = legend.cols))+
      new_scale_fill()+
      geom_label(aes(label=stats_label,fill=category),
                 position = position_stack(vjust = 0.5),size=4.5,
                 color="black",label.size = NA,label.padding = unit(0.1, "lines"),label.r = unit(0, "lines"))+
      scale_fill_manual(values=rep("#FFFFFFC8",times = nrow(df)))+
      guides(fill="none")+ylab("")+
      scale_y_continuous(expand = expansion(mult = c(0, .1)))+
      ggtitle(title)+
      theme_light(base_size = 20) +
      theme(panel.border = element_blank(),
            panel.grid = element_blank(),
            axis.line = element_line(linewidth = 0.5),
            axis.line.y = element_blank(),
            axis.title.x = element_blank(),
            axis.title.y=element_blank(),
            axis.text.y=element_blank(),
            axis.ticks=element_blank(),
            legend.justification = "center",
            legend.position = legend.position,
            legend.text = element_text(size=12),
            plot.title = element_text(hjust = 0.5),
            strip.placement = "outside",
            strip.background = element_rect(color="white", fill="white", linewidth=0.8, linetype="solid"),
            strip.text = element_text(colour = "black",face = "bold"))
  }else if(type == 2){
    ggplot(df, aes(x = month, y = stats)) +
      geom_bar(mapping = aes(fill = category),fill=color,width = 0.8, stat = "identity") +
      facet_grid(. ~ year, scales = "free", switch = "x", space = "free_x") +
      geom_text(aes(group=1,label=stats_label),stat="identity",vjust = -1,size=5,color="black")+
      ylab("") +
      scale_y_continuous(expand = expansion(mult = c(0, .1)))+
      theme_light(base_size = 20) +
      ggtitle(title)+
      theme(panel.border = element_blank(),
            panel.grid = element_blank(), axis.line = element_line(linewidth = 0.5),
            axis.line.y = element_blank(), axis.title.x = element_blank(),
            axis.title.y = element_blank(), axis.text.y = element_blank(),
            plot.title = element_text(hjust = 0.5),
            axis.ticks = element_blank(), legend.justification = "center",
            legend.position = "none",
            legend.text = element_text(size = 12),strip.placement = "outside",
            strip.background = element_rect(color = "white",fill = "white", linewidth = 0.8, linetype = "solid"),
            strip.text = element_text(colour = "black", face = "bold"))
  }else if(type == 3){
    month_year <- function(sep="/"){
      dates <- seq(as_date("2000-01-01"),as_date("2099-01-01"),by="month")
      unique(
        paste0(
          factor(
            as.numeric(month(dates)),
            levels=1:12,
            labels = c("Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez")
          ),
          sep,
          str_sub(dates,3,4)
        )
      )

    }
    n_groups <- 2
    df |>
      mutate(group = factor(sort(rep(1:n_groups,n()/n_groups)),labels = c("YoY","Atual"),ordered = T)) |>
      mutate(month_year = factor(paste0(month,"/",str_sub(year,3,4)),levels = month_year())) |>

      #ggplot(df, aes(x = month, y = stats)) +
      ggplot(aes(x = month_year, y = stats)) +
      geom_bar(mapping = aes(fill = category),fill=color,width = 0.8, stat = "identity") +

      facet_grid(. ~ group, scales = "free", switch = "x", space = "free_x") +
      geom_text(aes(group=1,label=stats_label),stat="identity",vjust = -1,size=5,color="black")+
      ylab("") +
      scale_y_continuous(expand = expansion(mult = c(0, .1)))+
      theme_light(base_size = 20) +
      ggtitle(title)+
      theme(panel.border = element_blank(),
            panel.grid = element_blank(), axis.line = element_line(linewidth = 0.5),
            axis.line.y = element_blank(), axis.title.x = element_blank(),
            axis.title.y = element_blank(), axis.text.y = element_blank(),
            plot.title = element_text(hjust = 0.5),
            axis.ticks = element_blank(), legend.justification = "center",
            legend.position = "none",
            legend.text = element_text(size = 12),strip.placement = "outside",
            strip.background = element_rect(color = "white",fill = "white", linewidth = 0.8, linetype = "solid"),
            strip.text = element_text(colour = "black", face = "bold"))
  }
}

#' @title Gera plot de volume diário
#' @name plot_daily_volume
#'
#' @param df Dataframe de estatísticas formatado.
#' @param min.share.label Valor mínimo para a legenda ser exibida, default: 0.03.
#' @param legend.position posição da legenda ("top","bottom","left","right","none"), default: top.
#' @param legend.cols quantidade de colunas de legenda, default: 3.
#'
#' @return Plot de volume
#'
#' @author Thiago Miranda
#'
#' @import dplyr
#' @import ggplot2
#' @import ggnewscale
#' @import ggh4x
#' @export
plot_daily_volume <- function(df,type=1,min.share.label=0.03,legend.position="top",legend.cols=3,color="#33AEB1",title=NULL){

  if(type == 1){
    colors <- get_colors(sort(unique(pull(df,category))))

    df <- df |>
      mutate(share_label = ifelse(share >=min.share.label, share_label, NA)) |>
      mutate(stats_label = ifelse(share >=min.share.label, stats_label, NA)) |>
      mutate(total = ifelse(share >=min.share.label, total, NA))

    # Volume Graph
    df |>
      ggplot(aes(x=day,y=stats))+
      geom_bar(mapping = aes(fill=category),width = 0.8,stat = "identity") +
      facet_nested(.~year+month, scales = "free", switch = "x", space = "free_x")+
      geom_text(aes(x=day, y=total,group=1,label=total_label),stat="identity",hjust=-0.1,size=4,color="black",angle = 90)+
      scale_fill_manual(values=colors,name=NULL,guide = guide_legend(ncol = legend.cols))+
      new_scale_fill()+
      scale_fill_manual(values=rep("#FFFFFFC8",times = nrow(df)))+
      guides(fill="none")+ylab("")+
      scale_y_continuous(expand = expansion(mult = c(0, .11)))+
      scale_x_discrete(breaks = str_pad(c(1,7,14,21,28),2,"left","0"))+
      theme_light(base_size = 20) +
      theme(panel.border = element_blank(),
            panel.grid = element_blank(),
            axis.line = element_line(linewidth = 0.5),
            axis.line.y = element_blank(),
            axis.title.x = element_blank(),
            axis.title.y=element_blank(),
            axis.text.y=element_blank(),
            axis.ticks=element_blank(),
            plot.title = element_text(hjust = 0.5),
            legend.justification = "center",
            legend.position = legend.position,
            legend.text = element_text(size=12),
            strip.placement = "outside",
            strip.background = element_rect(color="white", fill="white", linewidth=0.8, linetype="solid"),
            strip.text = element_text(colour = "black",face = "bold"))

  }else if(type == 2){
    df |>
      ggplot(aes(x=day,y=stats))+
      geom_bar(mapping = aes(fill=category),fill=color,width = 0.8,stat = "identity") +
      facet_nested(.~ year+month, scales = "free", switch = "x", space = "free_x")+
      geom_text(aes(x=day, y=total,group=1,label=total_label),stat="identity",hjust=-0.1,size=4,color="black",angle = 90)+
      ylab("")+
      scale_y_continuous(expand = expansion(mult = c(0, .11)))+
      scale_x_discrete(breaks = str_pad(c(1,7,14,21,28),2,"left","0"))+
      theme_light(base_size = 20) +
      ggtitle(title)+
      theme(panel.border = element_blank(),
            panel.grid = element_blank(),
            axis.line = element_line(linewidth = 0.5),
            axis.line.y = element_blank(),
            axis.title.x = element_blank(),
            axis.title.y=element_blank(),
            axis.text.y=element_blank(),
            axis.ticks=element_blank(),
            plot.title = element_text(hjust = 0.5),
            legend.justification = "center",
            legend.position = legend.position,
            legend.text = element_text(size=12),
            strip.placement = "outside",
            strip.background = element_rect(color="white", fill="white", linewidth=0.8, linetype="solid"),
            strip.text = element_text(colour = "black",face = "bold"))

  }

}

#' @title Gera plot de share diário
#' @name plot_daily_share
#'
#' @param df Dataframe de estatísticas formatado.
#' @param min.share.label Valor mínimo para a legenda ser exibida, default: 0.03.
#' @param legend.position posição da legenda ("top","bottom","left","right","none"), default: top.
#' @param legend.cols quantidade de colunas de legenda, default: 3.
#'
#' @return Plot de share
#'
#' @author Thiago Miranda
#'
#' @import dplyr
#' @import ggplot2
#' @import ggnewscale
#' @import ggtext
#' @export
plot_daily_share <- function (df, min.share.label=0.03, legend.position = "top", legend.cols = 3,title=NULL) {

  colors <- get_colors(sort(unique(pull(df, category))))

  df <- df |>
    mutate(share_label = ifelse(share >=min.share.label, share_label, NA)) |>
    mutate(stats_label = ifelse(share >=min.share.label, stats_label, NA))

  ggplot(df, aes(x = day, y = share)) +
    geom_bar(mapping = aes(fill = category),
             width = 0.8, stat = "identity") +
    facet_nested(.~year+month, scales = "free", switch = "x", space = "free_x")+
    scale_fill_manual(values = colors, name = NULL, guide = guide_legend(ncol = legend.cols)) +
    new_scale_fill() +
    geom_richtext(aes(label = share_label, fill = category),angle = 90,
                  position = position_stack(vjust = 0.5), size = 4.5, color = "black", label.size = NA,
                  label.padding = unit(0.1,"lines"), label.r = unit(0, "lines")) +
    scale_fill_manual(values = rep("#FFFFFFC8",times = nrow(df))) +
    guides(fill = "none") +
    ylab("") +
    scale_y_continuous(limits = c(0, 1.02), expand = c(0,0)) +
    theme_light(base_size = 20) +
    ggtitle(title)+
    theme(panel.border = element_blank(),
          panel.grid = element_blank(), axis.line = element_line(linewidth = 0.5),
          axis.line.y = element_blank(), axis.title.x = element_blank(),
          axis.title.y = element_blank(), axis.text.y = element_blank(),
          axis.ticks = element_blank(), legend.justification = "center",
          legend.position = legend.position,
          legend.text = element_text(size = 12),strip.placement = "outside",
          strip.background = element_rect(color = "white",fill = "white", linewidth = 0.8, linetype = "solid"),
          strip.text = element_text(colour = "black", face = "bold"))
}
