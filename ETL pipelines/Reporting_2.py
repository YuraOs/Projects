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
my_token = '5913206268:AAF4iQhxiB0dZ_L3'

#Подключение бота
bot = telegram.Bot(token=my_token)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['osipov'])
def pl():
    
    # Извлечение данных из таблицы за feed_actions
    @task()
    def extract_1():
        query_actions = """SELECT toDate(time) as date,
                            count(distinct user_id) as DAU,
                            sum(action == 'view') as views,
                            sum(action == 'like') as likes,
                            (sum(action == 'like') / sum(action = 'view')) as CTR
                            FROM base_2023.feed_actions
                            WHERE toDate(time) between today() -7 and today() 
                            GROUP BY date
                            ORDER BY date"""
        df_actions = ph.read_clickhouse(query_actions, connection=connection)
        return df_actions
    
    # Извлечение данных из таблицы за messages
    @task()
    def extract_2():
        query_messages = """SELECT toDate(time) as date,
                            count(distinct user_id) as DAU,
                            count(user_id) as sent_messages
                            FROM base_2023.message_actions
                            WHERE toDate(time) between today() - 7 and today() 
                            GROUP BY date
                            ORDER BY date"""
        df_messages = ph.read_clickhouse(query_messages, connection=connection)
        return df_messages
    
    # Создание единого отчета
    @task()
    def merge_tables():
        query_total = """select date, t1.DAU as DAU_actions, views, likes, CTR, t2.DAU as DAU_messages, sent_messages
                        from
                        (SELECT toDate(time) as date,
                        count(distinct user_id) as DAU,
                        sum(action == 'view') as views,
                        sum(action == 'like') as likes,
                        (sum(action == 'like') / sum(action = 'view')) as CTR
                        FROM base_2023.feed_actions
                        WHERE toDate(time) between today() -7 and today() 
                        GROUP BY date
                        ORDER BY date) t1
                        join
                        (SELECT toDate(time) as date,
                        count(distinct user_id) as DAU,
                        count(user_id) as sent_messages
                        FROM base_2023.message_actions
                        WHERE toDate(time) between today() - 7 and today() 
                        GROUP BY date
                        ORDER BY date) t2
                        using date"""
        df_total = ph.read_clickhouse(query_total, connection=connection)
        return df_total
    
   #Отправка графика по работе ленты новостей
    @task
    def actions_plot(df_actions):
        plt.figure(figsize=(10,10))
        plt.plot(df_actions['date'], df_actions[['likes', 'views', 'CTR', 'DAU']], marker='D')
        plt.title('feed_actions')
        plt.xlabel('day')
        plt.ylabel('amount')
        plt.legend(df_actions[['likes', 'views', 'CTR', 'DAU']])
        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.name = 'actions_table.png'
        plot_object.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    #Отправка графикв по работе мессенджера
    @task
    def massage_plot(df_messages):
        plt.figure(figsize=(10,10))
        plt.plot(df_messages['date'], df_messages[['sent_messages', 'DAU']], marker='D')
        plt.title('messager')
        plt.xlabel('day')
        plt.ylabel('amount')
        plt.legend(df_messages[['sent_messages', 'DAU']])
        plot_object1 = io.BytesIO()
        plt.savefig(plot_object1)
        plot_object1.name = 'actions_table.png'
        plot_object1.seek(0)
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object1)
        
    #Отправка общей сводки
    @task
    def massage_text(df_total):       
        file_object = io.StringIO()
        df_total.to_csv(file_object)
        file_object.name = 'total_data.csv'
        file_object.seek(0)
        bot.sendDocument(chat_id=chat_id, document=file_object)
        

    df_actions = extract_1()
    df_messages = extract_2()
    df_total = merge_tables()
    actions_plot(df_actions)
    massage_plot(df_messages)
    massage_text(df_total)

pl = pl()
