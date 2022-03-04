from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import os
import sys
import pytz
#sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
os.chdir('/home/mint/')
import testair1
import testair2
    
###################################################################################################################################################
######################################################### Создаем DAG #############################################################################
###################################################################################################################################################
yesterday = datetime.now(pytz.timezone("Asia/Vladivostok")).date() - timedelta(days=1)
yesterday = datetime.strftime(yesterday, '%Y-%m-%d')

default_args = {
    'owner': 'Khramenkov.VV@dns-shop.ru',
    'email': ['Khramenkov.VV@dns-shop.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('frcst_day22',
         start_date=datetime(2022, 2, 22, 7, 48, 0),
         schedule_interval=timedelta(minutes=1),
         default_args=default_args,
         description='Тестовая дичь python',
         tags=['frcst', 'kafka']) as dag:

    testair1 = PythonOperator(
        task_id='task_testair1',
        python_callable=testair1.main
        #,dag=dag
    )

    testair2 = PythonOperator(
        task_id='task_testair2',
        python_callable=testair2.main
        #,dag=dag
    )
    
    testair1 >> testair2
