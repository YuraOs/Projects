from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import requests
from io import StringIO
import telegram
import matplotlib.pyplot as plt
import io

connection = {'host': 'https://base_2023',
                      'database':'base_2023',
                      'user':'***',
                      'password':'***'
                     }

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'osipov',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2022, 10, 10),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

#Данные бота
chat_id = -802518
my_token = '5913206268:AAF4iQhxiB0dZ'

#Подключение бота
bot = telegram.Bot(token=my_token)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['osipov'])
def pl():
    
    # Извлечение данных из таблицы за сутки
    @task()
    def extract_1():
        query1 = """select toDate(time) as date,
                    sum(action='like') as likes,
                    sum(action='view') as views,
                    likes/views as CTR,
                    count(distinct user_id) as DAU
                    from base_2023.feed_actions
                    where toDate(time) = today() - 1
                    group by date"""
        df1 = ph.read_clickhouse(query1, connection=connection)
        return df1
    
    # Извлечение данных из таблицы за неделю
    @task()
    def extract_2():
        query2 = """select toDate(time) as date, sum(action='like') as likes,
                    sum(action='view') as views,
                    likes/views as CTR,
                    count(distinct user_id) as DAU
                    from base_2023.feed_actions
                    where toDate(time) between today()-7 and today()-1
                    group by date
                    order by date"""
        df_7days = ph.read_clickhouse(query2, connection=connection)
        return df_7days   
    
    
    @task
    def massage_text(df1):
        file_object = io.StringIO()
        df1.to_csv(file_object)
        file_object.name = 'yesterday_metrics.csv'
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object)

    @task
    def massage_plot(df_7days):
        plt.figure(figsize=(10,10))
        plt.plot(df_7days['date'], df_7days[['likes', 'views', 'CTR', 'DAU']], marker='D')
        plt.title('7days_metrics')
        plt.xlabel('day')
        plt.ylabel('amount')
        plt.legend(df_7days[['likes', 'views', 'CTR', 'DAU']])
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = '7days_metrics.png'
        plot_object.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
       

    df1 = extract_1()
    df_7days = extract_2()
    massage_text(df1)
    massage_plot(df_7days)
    
pl = pl()
    



    
    
    
    
    


    