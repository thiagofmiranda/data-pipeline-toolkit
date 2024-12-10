
import pandas as pd
import aips_gathering as aips

from joblib import Parallel, delayed
from tqdm.auto import tqdm
import os
import re
import s3fs
import pyarrow as pa
import pyarrow.dataset as ds

import json 
from datetime import date,datetime

from s3fs import S3FileSystem
s3fs = S3FileSystem()


from utils import *
from download_functions import *

def enrich_by_date(connection, get_by_date, source_folder,target_folder, target_dates, overwrite=False,n_threads=1):
  
  global parallel_save
  def parallel_save(query_date):
    query_df = get_by_date(connection, query_date, source_folder)
    
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
      
      
      
def enrich_applies_daily(conn, query_date,s3_path):
  
  df = read_dataset_by_date(query_date, s3_path)
  
  if df.shape[0] > 0:
    df_ids = get_applies_ids(conn)
    df_final = df.merge(df_ids, how='left', on="ID_ORGM_ENVO_CRRO")
    df_final['subscription'] = categorize_plans(df_final[['user_plan','user_plan_price']].copy())
    return df_final
  else:
    df['report_channel'] = []
    df['subscription'] = []
    return df

  
def enrich_apply_attempts_daily(conn, query_date,s3_path):
  
  df = read_dataset_by_date(query_date, s3_path)
  
  if df.shape[0] > 0:
    df_ids = get_applies_ids(conn)
    df_final = df.merge(df_ids, how='left', on="ID_ORGM_ENVO_CRRO")
    df_final['subscription'] = categorize_plans(df_final[['user_plan','user_plan_price']].copy())
    return df_final
  else:
    df['report_channel'] = []
    df['subscription'] = []
    return df
  
  
def enrich_contacts_daily(conn, query_date, s3_path):
  
  def download_user_contact_plan_daily(conn,query_date):
    query = f"""SELECT
                DATE(contato.DT_CNTO) as contact_date,
                contato.ID_USRO as usr_id,
                VL_PLNO as user_plan_price,
                CASE
                    WHEN DS_PLNO is NULL THEN 'none'
                    ELSE DS_PLNO
                END AS user_plan
            FROM USER_AREA_BI.TB_VIZ_CONTATO contato
                LEFT JOIN DWU.TB_SJT_CONFIGURACAO_COBRANCA_ASSINATURA cob
                    ON ( contato.ID_USRO = cob.ID_USRO AND cob.DT_INI_VGNA<= contato.DT_CNTO AND cob.DT_FIM_VGNA> contato.DT_CNTO)
                LEFT JOIN DWU.TB_SJT_PLANO_ASSINATURA_USUARIO pln
                    ON (pln.ID_PLNO = cob.ID_PLNO)
            WHERE
                contato.DT_CNTO >= '{query_date} 00:00:00'
            AND
                contato.DT_CNTO <= '{query_date} 23:59:59'"""
  
    df = pd.read_sql(query,conn)

    df['contact_date']   = pd.to_datetime(df['contact_date'].astype(str))

    df['user_plan'] = df['user_plan'].str.lower()
    df['user_plan_price'] = df['user_plan_price'].astype(float)

    df['subscription'] = categorize_plans(df[['user_plan','user_plan_price']].copy())

    df = df.sort_values('usr_id').drop_duplicates(['usr_id','contact_date'], keep='last')
  
    return df

  df = read_dataset_by_date(query_date, s3_path)
  
  if df.shape[0] > 0:
    df_contacts_type = get_contacts_ids(conn)
    df_contacts_type = df_contacts_type[['dim_tipo_contato_id','contact_type_id','descricao','origem']]
    df = df.merge(df_contacts_type, how='left', on=['dim_tipo_contato_id'])
  
    df_user_contact_plan = download_user_contact_plan_daily(conn,query_date)
    df_final = df.merge(df_user_contact_plan, how='left', on=['contact_date','usr_id'])
  
    return df_final
  else:
    df['contact_type_id'] = []
    df['descricao'] = []
    df['origem'] = []
    df['user_plan_price'] = []
    df['user_plan'] = []
    df['subscription'] = []
    return df
  
  

def enrich_job_organic_logged_searches_daily(conn, query_date,s3_path):
  def flag_paid_user(subscription):
    if subscription == 'Não Consta':
      return np.NaN
    elif subscription in ["Pro","Pagantes (antigo)","Pro Básico","Pro+"]:
      return 1
    elif subscription in ["Gratuito","Gratuito (antigo)"]:
      return 0
    else:
      return np.NaN
  
  def extract_query_classification(query_classification):
  
    def extract(string):
      try: 
        re.search("\w+(?=\:)",string).group() 
      except AttributeError as error: 
        result = ''
      else:
        result = re.search("\w+(?=\:)",string).group()
      finally:  
        return result
        
    result = list(map(lambda x: extract(x), query_classification.split(","))) 

    return result
  
  
  df = read_dataset_by_date(query_date, s3_path)
  
  if df.shape[0] > 0:
    df_filters = df.apply(lambda x: pd.Series(
      json.loads(x['filters']).values(),
      index=json.loads(x['filters']).keys()), 
      axis=1)

    df = pd.concat([df.reset_index(drop=True), df_filters], axis=1)

    df = preprocessor(df,'keywords',stemm=True)

    df['join_date'] = pd.to_datetime(df['search_date'].astype(str)+" 23:59:59.999") 

    ids_test = df['usr_id'].drop_duplicates()
    
    df_user_plan = get_user_plan_by_ids(conn, query_date, ids_test, 10000)
    #df_user_plan = get_user_plan(conn,query_date,query_date)# botar fora, outra funcao
    df_user_plan['usr_id'] = df_user_plan['usr_id'].astype(str)

    df_final = df.merge(df_user_plan, how='left', on=['usr_id'])

    df_final['fl_paid_user'] = df_final.apply(lambda x: flag_paid_user(x['subscription']), axis=1)

    df_final['query_classification_tags'] = df_final.apply(lambda x: extract_query_classification(x['query_classification']), axis=1).str.join('-')

    return df_final
  
  else:
    df['city_id'] = []
    df['contract_type_id'] = []
    df['exclude_aggregator'] = []
    df['hierarchical_level_id'] = []
    df['is_premium_job_ad'] = []
    df['ppd_profile_id'] = []
    df['professional_area_id'] = []
    df['recency_in_days'] = []
    df['region_id'] = []
    df['salary_range_id'] = []
    df['segment_id'] = []
    df['state_id'] = []
    df['work_model'] = []
    df['keywords_norm'] = []
    df['keywords_stem'] = []
    df['join_date'] = []
    df['plano_id'] = []
    df['user_plan'] = []
    df['user_plan_price'] = []
    df['subscription'] = []
    df['fl_paid_user'] = []
    df['query_classification_tags'] = [] 
    
    return df
    
  
  
def enrich_cv_organic_logged_searches_daily(conn, query_date,s3_path):

  df = read_dataset_by_date(query_date, s3_path)
  

  if df.shape[0] > 0:
    df_filters = df.apply(lambda x: pd.Series(
    json.loads(x['filters']).values(),
    index=json.loads(x['filters']).keys()), 
    axis=1)
  
    df = pd.concat([df.reset_index(drop=True), df_filters], axis=1)
  
    df = preprocessor(df,'keywords',stemm=True)
  
    return df
  
  else:
    df['age_id'] = []
    df['city_id'] = []
    df['country_id'] = []
    df['disability_profile_id'] = []
    df['filter_query'] = []
    df['gender'] = []
    df['geolocation'] = []
    df['hierarchical_level_id'] = []
    df['is_employed'] = []
    df['languages'] = []
    df['professional_area_id'] = []
    df['profile_id'] = []
    df['region_id'] = []
    df['salary_range_id'] = []
    df['state_id'] = []
    df['updated_date'] = []
    df['zone'] = []
    df['keywords_norm'] = []
    df['keywords_stem'] = []  
    
    return df