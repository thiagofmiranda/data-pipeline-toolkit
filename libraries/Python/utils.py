import boto3
import re
from itertools import compress
from datetime import date,datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute  as pc 
import pyarrow.dataset as ds

import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.snowball import SnowballStemmer
from nltk.stem.wordnet import WordNetLemmatizer
nltk.download("stopwords")
nltk.download("punkt")
nltk.download("wordnet")

def check_files_dont_exists(target_dates,s3_path):
  
  client = boto3.client('s3')

  prefix = re.search(r'(?<=\w{1}\/).*', s3_path).group()
  bucket = re.sub('(s3://)|/|'+prefix, '', s3_path)

  response = client.list_objects_v2(
      Bucket=bucket,
      Prefix=prefix)

  dates = []
  for content in response.get('Contents', []):
    if bool(re.search("parquet",content['Key'])):
      #files.append(content['Key'])
      year = re.search("(?<=year=).*(?=\\/month)",content['Key']).group()
      month = re.search("(?<=month=).*(?=\\/day)",content['Key']).group()
      day = re.search("(?<=day=).*(?=\\/)",content['Key']).group()
      
      dates.append(date(int(year),int(month),int(day)))
  
  check = []
  for i in target_dates:
    check.append(i not in dates)
  
  result = list(compress(target_dates, check))
  return result


def read_dataset_by_date(date,target_folder):
  f_year = pd.to_datetime(date).year
  f_month = pd.to_datetime(date).month
  f_day = pd.to_datetime(date).day
  
  dataset = ds.dataset(target_folder,
                       partitioning = ds.partitioning(
                         pa.schema(
                           [("year", pa.int16()),
                            ("month", pa.int16()),
                            ("day", pa.int16())]),
                         flavor="hive") 
                      )
  
  df = dataset.filter(
      (pc.field('year') == f_year) & (pc.field('month') == f_month) & (pc.field('day') == f_day)
  ).to_table().to_pandas()
  
  
  return df
  

def categorize_plans(df):
  
  def cat(user_plan,user_plan_price):
    if user_plan is None or bool(re.search("none",user_plan)):
      return 'Não Consta'
    elif bool(re.search("limitad",user_plan)):
      return 'Gratuito'
    elif user_plan_price > 0 and bool(re.search("basic",user_plan)):
      return 'Pro Básico'
    elif user_plan_price > 0 and bool(re.search(r"pro\+",user_plan)):
      return 'Pro+'
    elif user_plan_price > 0 and bool(re.search("upgrade",user_plan)):
      return 'Pro'
    elif user_plan_price > 0:
      return 'Pagantes (antigo)'
    elif user_plan_price == 0:
      return 'Gratuito (antigo)'
    else:
      return 'Não Consta'

  #df = pd.DataFrame({'user_plan': user_plan, 'user_plan_price': user_plan_price})
  
  df['user_plan'] = df['user_plan'].str.lower() #list(map(lambda x: x.lower, df['user_plan'])) 
  df['user_plan_price'] = df['user_plan_price'].astype(float)
  
  df['subscription'] = df.apply(lambda x: cat(x['user_plan'], x['user_plan_price']), axis=1)
  
  
  
  return df['subscription']


def clean_function(df, column_text):
  df[f'{column_text}_norm'] = df[[f'{column_text}']]\
  .replace(regex=r'[!/,.-]|"',value='')\
  .apply(lambda x: x.astype(str).str.lower())\
  .apply(lambda x: x.astype(str).str.strip())\
  .apply(lambda x: x.astype(str).str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8'))
  return df

def stop_words_function(df, column_name, new_column_name):
  stop_words = stopwords.words('portuguese')
  df[new_column_name] = df[column_name].apply(lambda x: ' '.join([word for word in x.split() if word not in (stop_words)]))
  return df

def tokenization (df,column_name, new_column_name):
  df[new_column_name] = df[column_name].map(lambda x: word_tokenize(x))
  return df

def stemming(df, column_name, new_column_name):
  snowball = SnowballStemmer(language = 'portuguese')
  df[new_column_name] = df[column_name].map(lambda x: [snowball.stem(y) for y in x])
  df[new_column_name] = df[new_column_name].str.join(' ') 
  return df

def preprocessor (df, column_name,stemm=True):
  cols = df.columns.values.tolist()
  
  data = clean_function(df, column_name)
  data = stop_words_function(data, f'{column_name}_norm', f'{column_name}_norm')
  
  if stemm:
    data = tokenization(data, f'{column_name}_norm', f'{column_name}_tokenized')
    data = stemming(data, f'{column_name}_tokenized', f'{column_name}_stem')
    cols.append(f'{column_name}_norm')
    cols.append(f'{column_name}_stem')
  else:
    cols.append(f'{column_name}_norm')
    
  return data[cols]