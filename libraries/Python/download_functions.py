

import pandas as pd
import numpy as np
import aips_gathering as aips

from joblib import Parallel, delayed
from tqdm.auto import tqdm
import os
import re
import s3fs
import pyarrow as pa
import pyarrow.dataset as ds
from datetime import date,datetime

from s3fs import S3FileSystem
s3fs = S3FileSystem()


from utils import *

def download_by_date(connection, get_by_date, target_folder, target_dates, overwrite=False,n_threads=1):
  
  global parallel_save
  def parallel_save(query_date):
    query_df = get_by_date(connection, query_date)
    
    query_df = pa.Table.from_pandas(query_df, preserve_index=False)
    
    ds.write_dataset(
      query_df, 
      target_folder,
      filesystem=s3fs, 
      format="parquet", 
      partitioning=ds.partitioning(pa.schema([("year", pa.int16()),("month", pa.int16()),("day", pa.int16())]),flavor="hive"),
      #partitioning_flavor="hive",
      existing_data_behavior="overwrite_or_ignore"
    )
  
  if overwrite:
    print("Overwriting data...")
    Parallel(n_jobs=n_threads,prefer="threads")(delayed(parallel_save)(query_date) for query_date in tqdm(target_dates))
  else:
    print("Writing only new data...")
    target_dates = check_files_dont_exists(target_dates,target_folder)
    if len(target_dates)>0:
      Parallel(n_jobs=n_threads,prefer="threads")(delayed(parallel_save)(query_date) for query_date in tqdm(target_dates))
    else:
      print("There is no file to be written.")
      


def download_applies_daily(conn, query_date):
  query = f"""
    SELECT
      DISTINCT env.ID_ENVO_CRRO,
      env.DT_ENVO_CRRO apply_datetime,
      env.ID_USRO as usr_id,
      env.ID_VAGA as vag_id,
      env.ID_ORGM_ENVO_CRRO,
      plano.DS_PLNO as user_plan,
      plano.VL_PLNO as user_plan_price,
      env.ID_DSPO_ENVO_CRRO,
      env.score
    FROM USER_AREA_BI.TB_VIZ_ENVIO_CURRICULO env
      LEFT JOIN DWU.TB_SJT_PLANO_ASSINATURA_USUARIO plano USING (ID_SJT_PLNO_ASSA_USRO)
    WHERE
      env.DT_ENVO_CRRO >='{query_date} 00:00:00' and
      env.DT_ENVO_CRRO <='{query_date} 23:59:59'
    """
  
  df = pd.read_sql(query,conn)
  
  df['apply_datetime'] = pd.to_datetime(df['apply_datetime'].astype(str))
  df['apply_date']     = df['apply_datetime'].dt.date
  df['year']           = df['apply_datetime'].dt.year
  df['month']          = df['apply_datetime'].dt.month
  df['day']            = df['apply_datetime'].dt.day
  
  return df

def download_apply_attempts_daily(conn, query_date):
  query = f"""
    SELECT
      DISTINCT env.ID_EVE_METRICAS_CANDIDATURA,
      env.DT_ENVO_CRRO apply_attempt_datetime,
      env.ID_USRO as usr_id,
      env.ID_VAGA as vag_id,
      env.ID_ORGM_ENVO_CRRO,
      plano.DS_PLNO as user_plan,
      plano.VL_PLNO as user_plan_price,
      env.ID_DSPO_ENVO_CRRO,
      score apply_score
    FROM USER_AREA_BI.TB_VIZ_METRICAS_CANDIDATURA env
      LEFT JOIN DWU.TB_SJT_PLANO_ASSINATURA_USUARIO plano USING (ID_SJT_PLNO_ASSA_USRO)
    WHERE
      env.DT_ENVO_CRRO >='{query_date} 00:00:00' and
      env.DT_ENVO_CRRO <='{query_date} 23:59:59'
    """
  
  df = pd.read_sql(query,conn)
  
  df['apply_attempt_datetime'] = pd.to_datetime(df['apply_attempt_datetime'].astype(str))
  df['apply_attempt_date']     = df['apply_attempt_datetime'].dt.date
  df['year']                   = df['apply_attempt_datetime'].dt.year
  df['month']                  = df['apply_attempt_datetime'].dt.month
  df['day']                    = df['apply_attempt_datetime'].dt.day
  
  return df

def download_contacts_daily(conn, query_date):
  query = f"""
    SELECT
    	f_contatos_id,
      usr_id,
      vag_id,
      emp_id,
      recrutador_id,
      dim_tipo_contato_id,
      cur_id,
      data contact_date
    FROM
    	Data_Warehouse.f_contatos c
      LEFT JOIN
        Data_Warehouse.dim_data d ON c.dim_data_contato_id = d.dim_data_id
    WHERE
	    d.`data` ='{query_date}'
    """
  
  df = pd.read_sql(query,conn)
  
  df['contact_date']   = pd.to_datetime(df['contact_date'].astype(str))
  df['year']           = df['contact_date'].dt.year
  df['month']          = df['contact_date'].dt.month
  df['day']            = df['contact_date'].dt.day
  
  return df

def download_job_postings_daily(conn, query_date):
  query = f"""
    SELECT DISTINCT
          Data_Warehouse.f_inclusao_vagas.vag_id,
          Data_Warehouse.f_inclusao_vagas.emp_id,
          DATE(Data_Warehouse.f_inclusao_vagas.data_ativacao) as job_posting_date,
          Data_Warehouse.dim_atendente.organizacao_inclusao organizacao_inclusao
        FROM Data_Warehouse.f_inclusao_vagas
          LEFT JOIN Data_Warehouse.dim_atendente ON Data_Warehouse.f_inclusao_vagas.dim_atendente_id = Data_Warehouse.dim_atendente.dim_atendente_id
        WHERE
          Data_Warehouse.f_inclusao_vagas.data_ativacao BETWEEN '{query_date} 00:00:00' AND '{query_date} 23:59:59'
            AND Data_Warehouse.dim_atendente.organizacao_inclusao IN ('Espont. Catho','Interno Catho','API','Agregador','Convertida')
    """
  
  df = pd.read_sql(query,conn)
  
  df['job_posting_date']   = pd.to_datetime(df['job_posting_date'].astype(str))
  df['year']           = df['job_posting_date'].dt.year
  df['month']          = df['job_posting_date'].dt.month
  df['day']            = df['job_posting_date'].dt.day
  
  return df


def download_active_jobs_daily(conn, query_date):
  query = f"""
            select 
              distinct 
                fva.vag_id, da.organizacao_inclusao, dim_data_id_entrada, dim_data_id_ativacao, dex.data job_active_date, den.data data_entrada,dat.data data_ativacao
            from Data_Warehouse.f_vagas_ativas fva
              left join Data_Warehouse.dim_data dex ON dex.dim_data_id = fva.dim_data_id_extracao
              left join Data_Warehouse.dim_data den ON den.dim_data_id = fva.dim_data_id_entrada
              left join Data_Warehouse.dim_data dat ON dat.dim_data_id = fva.dim_data_id_ativacao
              left join Data_Warehouse.dim_atendente da ON da.dim_atendente_id = fva.dim_atendente_id
            where dex.`data` ='{query_date}'
          """
  
  df = pd.read_sql(query,conn)
  
  df['job_active_date']   = pd.to_datetime(df['job_active_date'].astype(str))
  df['year']              = df['job_active_date'].dt.year
  df['month']             = df['job_active_date'].dt.month
  df['day']               = df['job_active_date'].dt.day
  
  return df



def download_job_organic_logged_searches_daily(conn, query_date):
  query = f"""
    WITH A AS (
                SELECT
                    JSON_EXTRACT(request_body, '$.user.candidate_id') AS usr_id,
                    CAST(\"x-solr-jobids\" AS VARCHAR) AS jobids,
                    -- CAST(\"x-solr-jobids-aggregator\" AS VARCHAR) AS jobids_aggregator,
                    CAST(json_extract(request_body,'$.query.filters.jobs.is_premium_job_ad') AS BOOLEAN) AS is_pja,
                    date_format(from_iso8601_timestamp(\"@timestamp\") AT TIME ZONE 'America/Sao_Paulo', '%Y-%m-%d %H:%i:%s.%f') AS search_datetime,
                    CAST(JSON_EXTRACT(request_body, '$.api.origin') AS VARCHAR) AS origin,
                    json_extract_scalar(request_body, '$.browser.device') as browser_device,
                    CAST(json_extract(\"x-solr-numresults\",'$.jobAds') AS INTEGER) AS num_job_ads,
                    CAST(\"x-query-classification\" AS VARCHAR) AS query_classification,
                    json_extract(request_body, '$.query.keywords') as keywords,
                    CAST(\"x-is-subscriber\" AS BOOLEAN) AS is_subscriber,
                    CAST(json_extract(request_body, '$.query.filters.jobs') AS JSON) as filters,
                    year,month,day

                FROM
                    \"loglake\".\"logs\"
                WHERE
                    engine = 'catho-search-api'
                    AND CONCAT(year, '-', month, '-', day) = '{query_date}'
                    AND status = '200'
                    AND request_body IS NOT NULL
                    AND uri = '/1.0/search/jobs' AND
                    CAST(JSON_EXTRACT(request_body, '$.api.service') AS VARCHAR) = 'search'
                )
            SELECT
            usr_id,is_subscriber, jobids,is_pja, search_datetime, query_classification, keywords,filters,origin,browser_device, year, month, day
            FROM A
            WHERE
                LENGTH(jobids) > 0 AND
                \"num_job_ads\" > 0 AND
                (origin = 'job-search-aws' OR
                    origin = 'ios_catho_search' OR
                    origin = 'android_catho_search' OR
                    origin = 'catho_search' OR
                    origin = 'area_candidate_search')
    """
  
  df = aips.query_athena_catho(query)
  
  df['search_datetime'] = pd.to_datetime(df['search_datetime'].astype(str))
  df['search_date']     = df['search_datetime'].dt.date
  df['year']            = df['year'].astype(int)
  df['month']           = df['month'].astype(int)
  df['day']             = df['day'].astype(int)
  
  df['is_pja'] = (df['is_pja']==True)*1
  df['aux_id'] = pd.to_datetime(df['search_datetime']).astype(int)+np.arange(len(df))
  
  return df



def download_cv_organic_logged_searches_daily(conn, query_date):
  query = f"""
          WITH cte AS (
              SELECT
                  date_format(from_iso8601_timestamp(\"@timestamp\") AT TIME ZONE 'America/Sao_Paulo', '%Y-%m-%d %H:%i:%s.%f') AS search_datetime,
                  json_extract(request_body, '$.user.id') as recruiter_id,
                  json_extract(request_body, '$.user.company.id') as emp_id,
                  json_extract_scalar(request_body,'$.query.keywords') AS keywords,
                  CAST(json_extract(request_body, '$.query.filters.cvs') AS JSON) as filters,
                  CAST(json_extract(response_body_custom, '$.cvs') AS JSON) as cvs,
                  CAST(JSON_EXTRACT(response_body_custom, '$.cvs') AS ARRAY<JSON>) as rbc,
                  CAST(JSON_EXTRACT(request_body, '$.user.company.subscriptions') AS ARRAY<JSON>) as ucs,
                  CAST(\"x-solr-numresults\" AS INTEGER) num_cvs,
                  json_format(json_extract(request_body, '$.facets')) as facets,
                  CAST(json_extract(request_body, '$.api.origin') AS VARCHAR) as origin,
                  appname,
                  uri ,year,month,day
              FROM \"loglake\".\"logs\"
              WHERE
                  CONCAT(year, '-', month, '-', day) = '{query_date}' AND
                  appname = 'catho-talentsearch-api' AND
                  request_body IS NOT NULL AND
                  (uri LIKE '%/1.0/resumes/search') AND
                  status = '200'
              )
          SELECT
              search_datetime,origin,
              recruiter_id,
              emp_id,
              TRANSFORM(ucs, x -> JSON_EXTRACT_SCALAR(x, '$.name')) as emp_plans,
              TRANSFORM(ucs, x -> JSON_EXTRACT_SCALAR(x, '$.id')) as emp_plan_ids,
              keywords,
              IF(filters IS NOT NULL AND json_size(filters, '$') > 0, 1, 0) has_filters,filters,
              num_cvs,
              TRANSFORM(rbc, x -> JSON_EXTRACT_SCALAR(x, '$.cv_id')) as cv_ids,
              TRANSFORM(rbc, x -> JSON_EXTRACT_SCALAR(x, '$.usr_id')) as usr_ids,
              -- TRANSFORM(rbc, x -> JSON_EXTRACT_SCALAR(x, '$.score')) as cv_score,
              TRANSFORM(rbc, x -> JSON_EXTRACT_SCALAR(x, '$.standoutLevel')) as usr_standoutLevel_ids,year,month,day
          FROM cte
          WHERE
              origin = 'logged_search_site_prod' 
    """
  
  df = aips.query_athena_catho(query)
  
  df['search_datetime'] = pd.to_datetime(df['search_datetime'].astype(str))
  df['search_date']     = df['search_datetime'].dt.date
  df['year']            = df['year'].astype(int)
  df['month']           = df['month'].astype(int)
  df['day']             = df['day'].astype(int)
  
  df['aux_id'] = pd.to_datetime(df['search_datetime']).astype(int)+np.arange(len(df))
  
  return df


def download_standout_daily(conn, query_date):
  query = f"""
          WITH requests AS (
            SELECT
                row_number() OVER () AS request_id,
                date_format(from_iso8601_timestamp(\"@timestamp\") AT TIME ZONE 'America/Sao_Paulo', '%Y-%m-%d %H:%i:%s.%f') AS standout_datetime,
                concat(year, '-', month, '-', day) as daystamp,
                CAST(json_extract(response_body_custom, '$.positions') AS ARRAY(JSON)) AS positions_array,
                CAST(json_extract(response_body_custom, '$.errors') AS ARRAY(JSON)) AS errors_array,
                json_extract_scalar(request_body, '$.origin') AS origin,
                \"status\",
                year,month,day
            FROM
                \"loglake\".\"logs\"
            WHERE
                CONCAT(year, '-', month, '-', day) = '{query_date}'
                AND status = '200'
                AND request_body IS NOT NULL
                AND engine = 'catho-recommendation-b2b-cv_position'
                AND uri = '/cv_rank_position/'
          ) SELECT
                request_id,
                standout_datetime,
                daystamp,
                origin,
                json_extract_scalar(position_json, '$.job_id') AS job_id,
                json_extract_scalar(position_json, '$.candidate_id') AS user_id,
                json_extract_scalar(position_json, '$.cv_profile_id') AS profile_id,
                json_extract_scalar(position_json, '$.paying_position') AS paying_position,
                json_extract_scalar(position_json, '$[\"non-paying_position\"]') AS non_paying_position,
                json_extract_scalar(position_json, '$.score') AS score,
                json_extract_scalar(position_json, '$.position.100') AS position_100,
                json_extract_scalar(position_json, '$.position.200') AS position_200,
                json_extract_scalar(position_json, '$.position.300') AS position_300,
                json_extract_scalar(position_json, '$.position.400') AS position_400,
                json_extract_scalar(position_json, '$.label') AS label,
                json_extract_scalar(position_json, '$.total_of_cvs') AS total_cvs,
                TRANSFORM(errors_array, x -> JSON_EXTRACT_SCALAR(x, '$.job_id')) AS job_ids_error,
                TRANSFORM(errors_array, x -> JSON_EXTRACT_SCALAR(x, '$.error_id')) AS erro_ids,
                year,month,day
          FROM
            requests, UNNEST (positions_array) AS t(position_json)
        """
  
  df = aips.query_athena_catho(query)
  
  df['standout_datetime'] = pd.to_datetime(df['standout_datetime'].astype(str))
  df['standout_date']     = df['standout_datetime'].dt.date
  df['year']              = df['year'].astype(int)
  df['month']             = df['month'].astype(int)
  df['day']               = df['day'].astype(int)
  
  df['aux_id'] = pd.to_datetime(df['standout_datetime']).astype(int)+np.arange(len(df))
  
  return df




def download_logs_health_check_daily(conn, query_date):
  query_jobsearch = f"""
        SELECT engine,CONCAT(year, '-', month, '-', day) AS log_date,status,'busca_de_vagas' AS origin,count(*) as status_hit
        FROM (
            SELECT
                engine,year,month,day,uri,status
            FROM
                \"loglake\".\"logs\"
            WHERE
                CONCAT(year, '-', month, '-', day) = '{query_date}'
                AND engine = 'catho-search-api'
                AND NOT (uri LIKE '/1.0/search/jobs/related-search')
                AND NOT request_body LIKE '%comm_jobalert%')
        GROUP BY engine,year,month,day,status
        """


  query_recs_b2c_delivery = f"""
    SELECT engine, CONCAT(year, '-', month, '-', day) AS log_date, status,origin, count(*) as status_hit
    FROM (
        SELECT
            engine, year, month, day, status,
            COALESCE(CASE
                WHEN uri LIKE '%/v2/delivery/registry/sugestao_delivery_ativos%' THEN 'registry_sugestao_delivery_ativos'
                WHEN uri LIKE '%/v2/delivery/registry/sugestao_delivery_inativos%' THEN 'registry_sugestao_delivery_inativos'
                WHEN uri LIKE '%/v2/delivery/registry/aviso_de_vagas%' THEN 'registry_aviso_de_vagas'
                WHEN uri LIKE '%/v2/delivery/recs/sugestao_delivery_ativos%' THEN 'sugestao_delivery_ativos'
                WHEN uri LIKE '%/v2/delivery/recs/sugestao_delivery_inativos%' THEN 'sugestao_delivery_inativos'
                WHEN uri LIKE '%/v2/delivery/recs/aviso_de_vagas%' THEN 'aviso_de_vagas'
            ELSE uri END, 'others') AS origin
        FROM
            \"loglake\".\"logs\"
        WHERE
            CONCAT(year, '-', month, '-', day) = '{query_date}'
            AND engine = 'catho-recommendation-b2c-delivery'
            AND (uri LIKE '%/v2/delivery/registry/sugestao_delivery_ativos%'
            OR uri LIKE '%/v2/delivery/registry/sugestao_delivery_inativos%'
            OR uri LIKE '%/v2/delivery/registry/aviso_de_vagas%'
            OR uri LIKE '%/v2/delivery/recs/sugestao_delivery_ativos%'
            OR uri LIKE '%/v2/delivery/recs/sugestao_delivery_inativos%'
            OR uri LIKE '%/v2/delivery/recs/aviso_de_vagas%'))
    GROUP BY engine,year,month,day,status,origin
    """
  query_recs_b2c = f"""
        SELECT engine,CONCAT(year, '-', month, '-', day) AS log_date,status,origin,count(*) as status_hit
        FROM (
            SELECT
                engine,year,month,day,json_extract_scalar(request_body,'$.context.origin') AS origin,status
            FROM
                \"loglake\".\"logs\"
            WHERE
                CONCAT(year, '-', month, '-', day) = '{query_date}'
                AND engine = 'catho-recommendation-b2c-api'
        )
        GROUP BY
            engine,year,month,day,origin,status
        """

  query_recs_b2b = f"""
        SELECT
            engine,CONCAT(year, '-', month, '-', day) AS log_date,status,origin,count(*) as status_hit
        FROM (
            SELECT
                engine,year,month,day,json_extract_scalar(request_body,'$.context.origin') AS origin,status
            FROM
                \"loglake\".\"logs\"
            WHERE
                CONCAT(year, '-', month, '-', day) = '{query_date}'
                AND engine = 'catho-recommendation-b2b-api')
        GROUP BY engine,year,month,day,origin,status
    """

  query_similar_jobs = f"""
        SELECT
            engine,CONCAT(year, '-', month, '-', day) AS log_date,status,'similar_jobs' AS origin,count(*) as status_hit
        FROM (
            SELECT
                engine,year,month,day,uri,status
            FROM
                \"loglake\".\"logs\"
            WHERE
                CONCAT(year, '-', month, '-', day) = '{query_date}'
                AND engine = 'catho-similar-jobs')
        GROUP BY
            engine,year,month,day,status
    """

  query_recs_b2b_scorer = f"""
    SELECT
        engine,CONCAT(year, '-', month, '-', day) AS log_date,status,'vagas_gratis' AS origin,count(*) as status_hit
    FROM (
        SELECT
            engine,year,month,day,status
        FROM
            \"loglake\".\"logs\"
        WHERE
            CONCAT(year, '-', month, '-', day) = '{query_date}'
            AND engine = 'catho-recommendation-b2b-scorer')
    GROUP BY
        engine,year,month,day,status
    """

  query_talent_search = f"""
    SELECT
        engine,CONCAT(year, '-', month, '-', day) AS log_date,status,'talent_search' AS origin,count(*) as status_hit
    FROM (
        SELECT
            engine,year,month,day,status
        FROM
            \"loglake\".\"logs\"
        WHERE
            CONCAT(year, '-', month, '-', day) = '{query_date}'
            AND engine = 'catho-talentsearch-api')
    GROUP BY
        engine,year,month,day,status
    """

  query_auto_apply = f"""
    SELECT
        engine,CONCAT(year, '-', month, '-', day) AS log_date,
        status,'auto_apply' as origin, count(*) as status_hit
    FROM (
        SELECT
            engine,year,month,day,status
        FROM
            \"loglake\".\"logs\"
        WHERE
            CONCAT(year, '-', month, '-', day) = '{query_date}'
            AND engine = 'catho-search-api'
            AND uri LIKE '/1.0/search/jobs/auto-apply')
    GROUP BY
        engine,year,month,day,status
    """

  query_job_alert = f"""
    SELECT
        engine,CONCAT(year, '-', month, '-', day) AS log_date,status,'aviso_de_vagas' AS origin,count(*) as status_hit
    FROM (
        SELECT
            engine,year,month,day,uri,status
        FROM
            \"loglake\".\"logs\"
        WHERE
            CONCAT(year, '-', month, '-', day) = '{query_date}'
            AND engine = 'catho-search-api'
            AND request_body LIKE '%comm_jobalert%'
    )
    GROUP BY
        engine,year,month,day,status
    """
  
  df_jobsearch         = aips.query_athena_catho(query_jobsearch)
  df_recs_b2c_delivery = aips.query_athena_catho(query_recs_b2c_delivery)
  df_recs_b2c          = aips.query_athena_catho(query_recs_b2c)
  df_recs_b2b          = aips.query_athena_catho(query_recs_b2b)
  df_similar_jobs      = aips.query_athena_catho(query_similar_jobs)
  df_recs_b2b_scorer   = aips.query_athena_catho(query_recs_b2b_scorer)
  df_talent_search     = aips.query_athena_catho(query_talent_search)
  df_auto_apply        = aips.query_athena_catho(query_auto_apply)
  df_job_alert         = aips.query_athena_catho(query_job_alert)
  
  df = pd.concat([
    df_jobsearch,
    df_recs_b2c_delivery,
    df_recs_b2c,
    df_recs_b2b,
    df_similar_jobs,
    df_recs_b2b_scorer,
    df_talent_search,
    df_auto_apply,
    df_job_alert
  ])
  
  def to_channel(origin,engine):
    if origin in ['busca_de_vagas']:
      res = 'Busca de Vagas'
    elif origin in ['sugestao','app_home_candidate','sugestao_mobile']:
      res = 'Sugestão de Vagas'
    elif origin in ['registry_sugestao_delivery_ativos','sugestao_delivery_ativos']:
      res = 'Sugestão Delivery'
    elif (origin in ['aviso_de_vagas']) and (origin in ['catho-search-api']):
      res = 'Aviso de Vagas'
    elif (origin in ['aviso_de_vagas','registry_aviso_de_vagas']) and (origin in ['catho-recommendation-b2c-delivery']):
      res = 'Aviso de Vagas'
    elif origin in ['similar_jobs']:
      res = 'Vagas Similares'
    elif origin in ['auto_apply']:
      res = 'Auto-Apply'
    elif origin in ['vagas_gratis']:
      res = 'Vagas Gratis'
    elif origin in ['triagem-auto']:
      res = 'Triagem'
    elif origin in ['rp-light']:
      res = 'RP Light'
    elif origin in ['talent_search']:
      res = 'Recrutamento Perfeito'
    else:
      res = 'Outros'
    return res

  def to_status_label(status):
    if status in range(100,200):
      res = 'Information'
    elif status in range(200,300):
      res = 'Success'
    elif status in range(300,400):
      res = 'Redirect'
    elif status in range(400,500):
      res = 'Client Error'
    elif status in range(500,600):
      res = 'Server Error'
    else:
      res = None
    return res
      
  df['status'] = df['status'].astype(int)
  df['channel'] = df.apply(lambda x: to_channel(origin = x['origin'], engine = x['engine']), axis=1)
  df['status_label'] = df.apply(lambda x: to_status_label(status = x['status']), axis=1)
      
  df['log_date'] = pd.to_datetime(df['log_date'].astype(str))
  df['year']            = df['log_date'].dt.year
  df['month']           = df['log_date'].dt.month
  df['day']             = df['log_date'].dt.day
  
  df = df.groupby(['log_date','day','month','year','channel','status_label'], as_index=False).agg(status_hit = ('status_hit',"sum"))
  
  return df

def get_applies_ids(conn,filter_channel=None):
  query = """SELECT
			ID_ORGM_ENVO_CRRO,
			CASE
				WHEN channel = \"\" Then 'Outros'
				WHEN channel = 'auto-apply' THEN 'Auto-Apply'
				WHEN CD_CNAL_ENVO_CRRO = 'super-apply' THEN 'Super-Apply'
				WHEN (channel REGEXP 'busca-de-vagas|caixa-de-busca-home-logada') THEN 'Busca de Vagas'
		      WHEN (channel REGEXP 'sugestao-de-vagas|app-recomendacao-vagas|app-sugestao-de-vagas|recomendacao-de-vagas') THEN 'Sugestão de Vagas'
		      WHEN (channel REGEXP 'sugestao-delivery') THEN 'Sugestão Delivery'
		      WHEN (channel REGEXP 'avisos-e-buscas-salvas') THEN 'Aviso de Vagas'
		      WHEN (channel REGEXP 'aviso-delivery') THEN 'Aviso de Vagas' # e-mail
		      WHEN (channel REGEXP 'convite-candidato-compativel') THEN 'Push Notification'
		      WHEN (channel REGEXP 'vagas-similares') THEN 'Vagas Similares'
		      ELSE 'Outros'
			END as report_channel
		FROM (
			SELECT
			ID_ORGM_ENVO_CRRO,
			CD_ORGM_ENVO_CRRO,
			CD_CNAL_ENVO_CRRO,
			CD_ORGM_ACSO,
			CASE
				WHEN (CD_ORGM_ENVO_CRRO REGEXP 'sugestao') AND  (CD_ORGM_ACSO REGEXP 'email') THEN 'sugestao-delivery'
				WHEN (CD_ORGM_ENVO_CRRO REGEXP 'aviso') AND (CD_ORGM_ACSO REGEXP 'email') THEN 'aviso-delivery'
				WHEN (CD_ORGM_ENVO_CRRO REGEXP 'aviso') AND (CD_ORGM_ACSO NOT REGEXP 'email') THEN 'avisos-e-buscas-salvas'
				WHEN (CD_ORGM_ENVO_CRRO REGEXP 'app-busca') THEN 'busca-de-vagas'
				ELSE CD_ORGM_ENVO_CRRO
			END as channel
			FROM DWU.TB_ATR_ORIGEM_ENVIO_CURRICULO
			ORDER BY ID_ORGM_ENVO_CRRO
		) AS tb"""
  
  df = pd.read_sql(query,conn)

  if filter_channel == None:
    return df
  else:
    df[df['report_channel'] == filter_channel]
    return df['ID_ORGM_ENVO_CRRO']
  
  
def get_contacts_ids(conn):
  query = """
  SELECT
    distinct dim_tipo_contato_id,descricao,origem
  FROM
    Data_Warehouse.dim_tipo_contato tp"""

  df = pd.read_sql(query,conn)
  df['descricao'] = df['descricao'].apply(lambda x: x.strip())
  df['origem'] = df['origem'].apply(lambda x: x.strip())
  df.sort_values('dim_tipo_contato_id')
  
  df_final = df[['origem','descricao']].drop_duplicates()
  
  df_final['contact_type_id'] = range(1,len(df_final)+1)
  
  df_final = df_final.merge(df, how='right', on=["origem","descricao"])
  
  return df_final[["dim_tipo_contato_id","origem","descricao","contact_type_id"]]

  
def get_user_plan_by_ids(conn,query_date,ids,bulk_size):
  query = f"""
      SELECT 
          ID_USRO usr_id,ID_PLNO plano_id, user_plan, user_plan_price
      FROM DWU.TB_SJT_CONFIGURACAO_COBRANCA_ASSINATURA cca
      LEFT JOIN (
          SELECT plano_id, nome user_plan, valor user_plan_price FROM Data_Warehouse.dim_plano p
          WHERE '{query_date} 23:59:59' between date_from AND date_to
      ) pln ON pln.plano_id = cca.ID_PLNO
      WHERE '{query_date} 23:59:59' between cca.DT_INI_VGNA AND cca.DT_FIM_VGNA AND
      """+"ID_USRO IN ({})"
  
  df = aips.get_sql_data_by_ids(
      conn, 
      ids, 
      query,
      bulk_size = bulk_size
  )

  df['user_plan'] = df['user_plan'].str.lower()
  df['user_plan_price'] = df['user_plan_price'].astype(float)

  df['subscription'] = categorize_plans(df[['user_plan','user_plan_price']].copy())
  
  return df