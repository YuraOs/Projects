# coding=utf-8
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import pandas as pd
import requests
import pandahouse as ph
from io import StringIO

connection = {'host': 'https://base_2023',
                      'database':'base_2023',
                      'user':'***',
                      'password':'***'
                     }
connection_to_test = {'host': 'https://base_2023',
                      'database':'test',
                      'user':'***',
                      'password':'***'
                     }

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'osipov',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 10, 10),
}

# Интервал запуска DAG
schedule_interval = '0 14 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['osipov'])
def pl():

    # Извлечение данных из таблицы actions
    @task()
    def extract_actions():
        query1 = """SELECT user_id, toDate(time) as event_date,
                    age, gender, os,
                    sum(action = 'like') as likes,
                    sum(action = 'view') as views
                    FROM base_2023.feed_actions
                    WHERE  event_date = today() - 1
                    GROUP BY user_id, event_date, age, gender, os"""
        df_actions = ph.read_clickhouse(query1, connection=connection)
        return df_actions
    
    @task()
    def extract_messages():
        query2 = """SELECT user_id,
                    t1.event_date,
                    age, gender, os,
                    t2.messages_received,
                    t1.messages_sent,
                    t1.users_sent,
                    t2.users_received
                    FROM
                    (SELECT user_id,
                    toDate(time) as event_date,
                    age, gender, os,
                    COUNT(user_id) as messages_sent,
                    COUNT(DISTINCT user_id) as users_sent
                    FROM base_2023.message_actions
                    WHERE  event_date = today() - 1
                    GROUP BY user_id, event_date, age, gender, os) as t1
                    LEFT JOIN 
                    (SELECT reciever_id,
                    toDate(time) as event_date,
                    COUNT(user_id) as messages_received,
                    COUNT(DISTINCT user_id) as users_received
                    FROM base_2023.message_actions
                    WHERE  event_date = today() - 1
                    GROUP BY reciever_id, event_date) as t2 
                    ON t1.user_id=t2.reciever_id"""
        df_messages = ph.read_clickhouse(query2, connection=connection)
        return df_messages
        
     # Объединие двух таблиц.
    @task()
    def merge_df(df_actions,df_messages):
        df1 = df_actions.merge(df_messages, how='outer', on=['user_id', 'event_date','age', 'gender','os'])
        return df1
    
    # Сделаем срез по OS.
    @task
    def transform_os(df1):
        df_os = df1.groupby(['event_date','os'])['views','likes','messages_received','messages_sent','users_received','users_sent'].sum().reset_index()
        df_os['measure'] = 'os'
        df_os.rename(columns = {'os' : 'measure_value'}, inplace = True)
        return df_os

    @task
    def transform_gender(df1):
        df_gender = df1.groupby(['event_date','gender'])['views','likes','messages_received','messages_sent','users_received','users_sent'].sum().reset_index()
        df_gender['measure'] = 'gender'
        df_gender.rename(columns = {'gender' : 'measure_value'}, inplace = True)
        return df_gender
    
    @task
    def transform_age(df1):
        df_age = df1.groupby(['event_date','age'])['views','likes','messages_received','messages_sent','users_received','users_sent'].sum().reset_index()
        df_age['measure'] = 'age'
        df_age.rename(columns = {'age' : 'measure_value'}, inplace = True)
        return df_age

    # Объединение единую таблицу    
    @task()
    def concat_all(df_os, df_gender, df_age):
        df_all = pd.concat([df_age, df_os, df_gender])
        df_all = df_all[['event_date', 'measure', 'measure_value', 'views', 'likes', 'messages_received', 'messages_sent', 'users_received', 'users_sent']]
        df_all = df_all.astype({'event_date' : 'datetime64',
                                'measure' : 'str',
                                'measure_value' : 'str',
                                'views' : 'int64',
                                'likes' : 'int64',
                                'messages_received' : 'int64',
                                'messages_sent' : 'int64',
                                'users_received' : 'int64',
                                'users_sent' : 'int64'})
        return df_all  
    
    # Выгрузка таблицы       
    @task
    def load(df_all):
        new_query = '''CREATE TABLE IF NOT EXISTS test.osipov
        (event_date Date,
         measure String,
         measure_value String,
         views Int64,
         likes Int64,
         messages_received Int64,
         messages_sent Int64,
         users_received Int64,
         users_sent Int64
         ) ENGINE = MergeTree()
         ORDER BY event_date
         '''
        ph.execute(query=new_query, connection=connection_to_test)
        ph.to_clickhouse(df=df_all, table='j_osipov', connection=connection_to_test, index=False)
                
    df_actions = extract_actions()
    df_messages = extract_messages()
    df1 = merge_df(df_actions,df_messages)
    df_os = transform_os(df1)
    df_gender = transform_gender(df1)
    df_age = transform_age(df1)
    df_all = concat_all(df_os, df_gender,df_age)
    load(df_all)
    
pl = pl()
