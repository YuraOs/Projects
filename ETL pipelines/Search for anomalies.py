from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from datetime import datetime, timedelta
import pandas as pd
import pandahouse as ph
import requests
from io import StringIO
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
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
chat_id = -958942
my_token = '5913206268:AAF4iQhxiB0dZ'

#Подключение бота
bot = telegram.Bot(token=my_token)

#Функция межквартильный размах
def check_anomaly(df, metric, a = 4, n = 5):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a * df['iqr']
    df['low'] = df['q25'] - a * df['iqr']
    df['up'] = df['up'].rolling(n, center = True, min_periods=1).mean()
    df['low'] = df['low'].rolling(n, center = True, min_periods=1).mean()
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, df

#Формируем таблицу
q = '''select ts, date, hm, active_users, views, likes, CTR, sent_messages 
        from
        (select ts, date, hm, views, likes, CTR, sent_messages
        from
        (SELECT toStartOfFifteenMinutes(time) as ts,
        toDate(time) as date,
        formatDateTime(ts, '%R') as hm,
        countIf(user_id, action == 'view') as views,
        countIf(user_id, action == 'like') as likes,
        (sum(action == 'like') / sum(action = 'view')) as CTR
        FROM base_2023.feed_actions
        WHERE toDate(time) >= today() - 1 and  toDate(time) < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts, date, hm) t1
        join
        (SELECT  toStartOfFifteenMinutes(time) as ts,
        toDate(time) as date,
        formatDateTime(ts, '%R') as hm,
        count(user_id) as sent_messages
        FROM base_2023.message_actions
        WHERE toDate(time) >= today() - 1 and  toDate(time) < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts, date, hm) t2
        using ts, date, hm) tt1
        join
        (select toStartOfFifteenMinutes(time) as ts,
        toDate(time) as date,
        formatDateTime(ts, '%R') as hm, 
        count(user_id) as active_users
        from 
        (select user_id, time
        from
        (select user_id, time
        from base_2023.feed_actions
        WHERE time >= today() - 1 and  time < toStartOfFifteenMinutes(now())) t
        inner join
        (select user_id, time
        from base_2023.message_actions
        WHERE time >= today() - 1 and  time < toStartOfFifteenMinutes(now())) tt
        using user_id)
        group by ts, date, hm
        order by ts, date, hm) tt2
        using ts, date, hm
        order by ts'''


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False, tags=['osipov'])
def pl():
    
    #Формирование датафрейма
    @task()
    def extract_1():
        data = ph.read_clickhouse(q, connection=connection)
        return data
    
    # Поиск аномалий и отправка алертов
    @task()   
    def alert(data):
        metric_list = ['active_users', 'views', 'likes', 'CTR', 'sent_messages']
        for metric in metric_list:
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly(df, metric)

            if is_alert == 1:
                sns.set(rc={'figure.figsize':(16,10)})
                plt.tight_layout()
                ax = sns.lineplot(x=df['ts'], y = df[metric], label = 'metric')
                ax = sns.lineplot(x=df['ts'], y = df['up'], label = 'up')
                ax = sns.lineplot(x=df['ts'], y = df['low'], label = 'low')
                ax.set(xlabel = 'time')
                ax.set(ylabel = metric)
                ax.set_title(metric)
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.name = 'metric.png'
                plot_object.seek(0)
                plt.close()
                bot.sendPhoto(chat_id=chat_id, photo=plot_object)

                msg = '''Метрика {metric}: \n Текущее значение: {current_val:.2f}\n Отклонение от предыдущего значения: {last_val_diff:.2%}'''.format(metric=metric, current_val = df[metric].iloc[-1], last_val_diff = abs((1 - df[metric].iloc[-1] / df[metric].iloc[-2])))

                bot.sendMessage(chat_id=chat_id, text = msg)
        

    

    
    data = extract_1()
    alert(data)

pl = pl()