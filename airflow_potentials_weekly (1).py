from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
#The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
#importing the operators required
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

import test_kafka

from my_functions import execution,toSkype
import pytz
tz = pytz.timezone('Asia/Irkutsk')

from turnover_itself import check_new_categories
from turnover_sku_rrc import main

default_args = {
'owner' : 'airflow',
#'depends_on_past' : False,
'start_date' : datetime(2024, 4, 8, 9, 0, 0),
# помним, что начинается не сразу, а через заданный интервал
# поэтому дата начала такая, чтобы до первого выполнения оставалось меньше 24 часов (или день с хвостиком?)
# Задаём время по UTC (-8 от Иркутска)
'email' : ['example@123.com'],
'email_on_failure' : False,
'email_on_retry' : False,
'retries' : 0,
'retry_delay' : timedelta(minutes=1)
}
dag = DAG(
'potentials_update_weekly',
description = 'Большое обновление потенциалов',
default_args = default_args,
schedule_interval = timedelta(days=7)
,catchup=False
)

t1 = PythonOperator(
 task_id = 'check_new_categories', 
 python_callable=check_new_categories,
 dag = dag
)

"""
t2 = BashOperator(
 task_id = 'full_update', 
 bash_command="python3 /opt/airflow/dags/repo/dags/turnover_sku_rrc.py",
 dag = dag
)
"""
t2 = PythonOperator(
 task_id = 'full_update', 
 python_callable=main,
 dag = dag
)

t1 >> t2
