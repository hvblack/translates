from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
#The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
#importing the operators required
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from my_functions import execution,toSkype,df_build,df_load
import pytz
tz = pytz.timezone('Asia/Vladivostok')
from time import sleep

import pandas as pd

import os 
os.chdir(os.path.dirname(__file__)) 

from turnover_itself import check_new_categories

def update_potentials():
    import turnover_sku as ts

    scr="""
        select toStartOfWeek(date_day,1) ds,date_day,main.branch_code "BranchCode" ,category_4_id category_guid,
            sum("general") "general",sum(reserve) "reserve",sum("exposition") "exposition"
            -- select rrc_name,count(distinct branch_code)
        from formats.mart_additional_movements_for_hubs main 
            left join dict.product product on main.product_code = product.code
            left join dict.branch branch on main.branch_code =toString(branch.code)
        where 1==1--category_4_id='fd72100d-70e6-11e2-b24e-00155d030b1f'
            and ds<=Now()::date-Interval '7 day'
            and ds>=Now()::date-Interval '7 day'-Interval '28 day'
            --and main.branch_code in (@branches)
            and branch.type_name in ('Магазин','Дисконт центр')
            --and 1=2
        group by ds,date_day,main.branch_code,category_guid
        """
    temp=df_build('adm_fcs4',scr)
    temp['dt']=datetime.now(tz)
    df_load('delta_bi','public.turnover_hubs',temp)

    if datetime.now(tz).isoweekday() in [11]: # по понедельникам - полный перегруз - перенёс в отдельный даг
        

        #ts.create_SDP_sales(comm='by cat year smooth',months=3)
        #toSkype('Потенциалы\n\nобновили сценарии продаж','Лог')
        
        
    #if datetime.now(tz).month!=(datetime.now(tz)+timedelta(days=7)).month : # последний понедельник месяца

        x=datetime.now()
        ts.create_potentials(add=False,use_params=True,write_params=True,write_metrics=True,write_time_metrics=True,beforeafter=True,load_cleared_sku=True)
        toSkype('Потенциалы\n\nвычислили потенциалы','Лог')
        #ts.upload_potentials_all_in_one(types=['sku','items'])
        #toSkype('Потенциалы\n\nзалили полный набор','Лог')
        
        #else:
        #    ts.create_potentials(add=False,old_sku=True)
        #    toSkype('Потенциалы\n\nвычислили потенциалы со старыми СКУ','Лог')
        #    ts.upload_potentials_all_in_one(types=['items'])
        #    toSkype('Потенциалы\n\nзалили штуки','Лог')

        #ts.upload_PP()
        #toSkype('Потенциалы\n\nзалили ПП Оборот','Лог')

        y=datetime.now()
        ts.write_to_log(1,int((y-x).total_seconds()//60))

        ts.compare_branch_model(update=True) # перезаписываем филиалы и модели

    elif datetime.now(tz).isoweekday() in [2,3,4,5,6,7]: # по остальным будням - точечно при изменении параметров
        

        x=datetime.now()
        ts.daily_update()
        y=datetime.now()

        scr="""
        select distinct "BranchCode" ,"CategoryCode" 
        from public.turnover_potentials
        """
        temp=df_build('delta_bi',scr)
        if temp.empty:
            ts.write_to_log(0,0)
        else:
            ts.write_to_log(1,int((y-x).total_seconds()//60))

        
    else:
        ts.write_to_log(0,0)

    # Первое воскресенье месяца - обновляем оборачиваемость
    if datetime.now(tz).isoweekday() in [7,] and datetime.now(tz).month!=(datetime.now(tz)-timedelta(days=7)).month:
        # полностью обновляем оборачиваемость
        #comm='click_'+str(datetime.now())
        #ti.create_turnover_branch(comm) # вычисляем до филиалов
        #new_temp=ti.create_turnover_category(comm) # собираем до категорий
        #ti.upload_turnover(new_temp) # заливаем в Клик и 1С
        pass

default_args = {
'owner' : 'airflow',
#'depends_on_past' : False,
'start_date' : datetime(2023, 5, 17, 9, 0, 0),
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
'potentials_update4',
description = 'Обновление потенциалов',
default_args = default_args,
schedule_interval = timedelta(days=1)
,catchup=False
)

t1 = PythonOperator(
 task_id = 'check_new_categories', 
 python_callable=check_new_categories,
 dag = dag
)


t2 = PythonOperator(
 task_id = 'update_potentials', 
 python_callable=update_potentials,
 dag = dag
)

t1 >> t2
