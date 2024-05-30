from my_functions import df_build,df_load,toSkype,to_csv

import forecast_model_auto as f_auto
import forecast_model_prophet as f_prophet
import forecast_model_prophet_store_category as f_prophet_rrc
import forecast_model_auto_trend as f_trend

from forecast_functions import download_data,download_data_rrc,year_exclusion,compute_forecast_income_cost,compute_forecast_monthly,create_days28,download_data_rrc_product,break_to_rrc_product,create_ranked_models,download_data_rrc_d

import datetime
import pytz
tz = pytz.timezone('Asia/Irkutsk')

import pandas as pd
import numpy as np

import stan
from prophet import Prophet

from joblib import Parallel, delayed
import logging

import traceback

def cross_autoforecast(data,category_guid,future_date,lag):
    """
    вычисление прогноза для автопрогноза, обработка
    вход:
        sales_d_consistent (датафрейм) - однородные продажи день-категория
        sales_d (датафрейм) - продажи день-категория
        calendar (датафрейм) - календарь
        future_date - дата, чей прогноз используем для метрик качества
        lag - глубина прогнозирования. число периодов между датой составления прогноза и прогнозируемой датой
        skiped_periods (датафрейм) - недели-категории, которые надо исключать из расчёта
        category_guid - гуид категории
    выход:
        forecast (датафрейм) - прогноз
    """

    #toSkype(str(datetime.datetime.now())+' внутри кросс-автопрогноза начало\n '+category_guid,'Airflow')
    # условная дата запуска
    cutoff=future_date-datetime.timedelta(weeks=lag)
    
    weekly,monthly=f_auto.create_weekly_and_monthly(data,category_guid,cutoff)
    
    data['weekly']=weekly
    data['monthly']=monthly
    
    forecast=f_auto.main(data,category_guid,cutoff,horizon_weeks=lag+3)
    forecast=forecast[forecast.ds==future_date]

    forecast['lag']=lag
    forecast['cutoff']=cutoff
    forecast['category_guid']=category_guid
    #toSkype(str(datetime.datetime.now())+' внутри кросс-автопрогноза конец\n '+category_guid,'Airflow')
    return forecast 


def cross_autoforecast_rrc(data,category_guid,futures,lag):

    #toSkype(str(datetime.datetime.now())+' внутри кросс-автопрогноза начало\n '+category_guid,'Airflow')
    
    future_date=datetime.datetime.strptime(futures[0],'%Y-%m-%d')
    dt=pd.DataFrame([datetime.datetime.strptime(dt,'%Y-%m-%d') for dt in futures],columns=['ds'])

    # условная дата запуска
    cutoff=future_date-datetime.timedelta(weeks=lag)
    
    weekly,monthly=f_auto.create_weekly_and_monthly(data,category_guid,cutoff)
    
    data['weekly']=weekly
    data['monthly']=monthly
    
    forecast=f_auto.main(data,category_guid,cutoff,horizon_weeks=lag+3)
    forecast=forecast.merge(dt,how='inner',on='ds')

    forecast['lag']=lag
    forecast['cutoff']=cutoff
    forecast['category_guid']=category_guid
    #toSkype(str(datetime.datetime.now())+' внутри кросс-автопрогноза конец\n '+category_guid,'Airflow')
    return forecast 



def cross_prophet(data,category_guid,future_date,lag):
    """
    вычисление прогноза для автопрогноза, обработка
    вход:
        sales_w (датафрейм) - продажи неделя-категория
        calendar (датафрейм) - календарь
        future_date - дата, чей прогноз используем для метрик качества
        lag - глубина прогнозирования. число периодов между датой составления прогноза и прогнозируемой датой
        category_guid - гуид категории
    выход:
        forecast (датафрейм) - прогноз
    """
    #toSkype(str(datetime.datetime.now())+' внутри кросс-профета начало\n '+category_guid,'Airflow')
    #calendar=data['calendar']
    
    cutoff=future_date-datetime.timedelta(weeks=lag)
    try:
        forecast=f_prophet.main(data,category_guid,cutoff,horizon_weeks=lag+3)
        forecast=forecast[forecast.ds==future_date]
    except:
        forecast=pd.DataFrame({'ds':future_date,'yhat':0,'yhat_lower':-10000,'yhat_upper':0}, index=[0])

    forecast['lag']=lag
    forecast['cutoff']=cutoff
    forecast['category_guid']=category_guid
    #toSkype(str(datetime.datetime.now())+' внутри кросс-профета конец\n '+category_guid,'Airflow')
    return forecast
    
def cross_prophet_rrc_old(data,category_guid,future_date,lag):
    """
    вычисление прогноза для автопрогноза, обработка
    отличия от обычной - вычисление сразу всех дат на одном лаге + изменённая yearly_seasonality
    вход:
        sales_w (датафрейм) - продажи неделя-категория
        calendar (датафрейм) - календарь
        future_date - дата, чей прогноз используем для метрик качества
        lag - глубина прогнозирования. число периодов между датой составления прогноза и прогнозируемой датой
        category_guid - гуид категории
    выход:
        forecast (датафрейм) - прогноз
    """
    #toSkype(str(datetime.datetime.now())+' внутри кросс-профета начало\n '+category_guid,'Airflow')
    #calendar=data['calendar']
    
    cutoff=future_date-datetime.timedelta(weeks=lag)
    try:
        forecast=f_prophet.main(data,category_guid,cutoff,yearly_seasonality=10,horizon_weeks=lag+3)
        forecast=forecast[forecast.ds==future_date]
    except:
        forecast=pd.DataFrame({'ds':future_date,'yhat':0,'yhat_lower':-10000,'yhat_upper':0}, index=[0])

    forecast['lag']=lag
    forecast['cutoff']=cutoff
    forecast['category_guid']=category_guid
    #toSkype(str(datetime.datetime.now())+' внутри кросс-профета конец\n '+category_guid,'Airflow')
    return forecast
    
def cross_prophet_rrc(data,category_guid,futures,lag):
    """
    вычисление прогноза для автопрогноза, обработка
    вход:
        future_date - дата, чей прогноз используем для метрик качества
        lag - глубина прогнозирования. число периодов между датой составления прогноза и прогнозируемой датой
        category_guid - гуид категории
    выход:
        forecast (датафрейм) - прогноз
    """
    
    future_date=datetime.datetime.strptime(futures[0],'%Y-%m-%d')
    # условная дата запуска
    cutoff=future_date-datetime.timedelta(weeks=lag)
    #cutoff=datetime.datetime.strptime(cutoff, '%Y-%m-%d')
    
    dt=pd.DataFrame([datetime.datetime.strptime(dt,'%Y-%m-%d') for dt in futures],columns=['ds'])
    
    
    try:
        forecast=f_prophet.main(data,category_guid,cutoff,yearly_seasonality=10,horizon_weeks=lag+3)
        forecast=forecast.merge(dt,how='inner',on='ds')
    except:
        forecast=pd.DataFrame({'ds':future_date,'yhat':0,'yhat_lower':-10000,'yhat_upper':0}, index=[0])
        #return forecast=pd.DataFrame({'ds':future_date,'yhat':0,'yhat_lower':-10000,'yhat_upper':0}, index=[0])

    forecast['lag']=lag
    forecast['cutoff']=cutoff
    forecast['category_guid']=category_guid

    return forecast 


def cross_trend1(data,category_guid,future_date,lag):
    
    cutoff=future_date-datetime.timedelta(weeks=lag)
    forecast=f_trend.main_Автопрогноз_53_3нТренд(data,category_guid,cutoff,lag+3) # плюс три - чтобы со всеми запасными отступлениями
    forecast=forecast[forecast.ds==future_date]

    forecast['lag']=lag
    forecast['cutoff']=cutoff
    forecast['category_guid']=category_guid
    return forecast
    
    
def cross_trend2(data,category_guid,future_date,lag):
    
    cutoff=future_date-datetime.timedelta(weeks=lag)
    forecast=f_trend.main_Автопрогноз_53_2_52_3нТренд(data,category_guid,cutoff,lag+3) # плюс три - чтобы со всеми запасными отступлениями
    forecast=forecast[forecast.ds==future_date]


    forecast['lag']=lag
    forecast['cutoff']=cutoff
    forecast['category_guid']=category_guid
    return forecast
    
    
def cross_trend3(data,category_guid,future_date,lag):
    
    cutoff=future_date-datetime.timedelta(weeks=lag)
    forecast=f_trend.main_Автопрогноз_53_4_52_3нТренд(data,category_guid,cutoff,lag+3) # плюс три - чтобы со всеми запасными отступлениями
    forecast=forecast[forecast.ds==future_date]


    forecast['lag']=lag
    forecast['cutoff']=cutoff
    forecast['category_guid']=category_guid
    return forecast
    
    
def cross_trend4(data,category_guid,future_date,lag):
    
    cutoff=future_date-datetime.timedelta(weeks=lag)
    forecast=f_trend.main_АвтопрогнозФормулаНакопСумма(data,category_guid,cutoff,lag+3) # плюс три - чтобы со всеми запасными отступлениями
    forecast=forecast[forecast.ds==future_date]


    forecast['lag']=lag
    forecast['cutoff']=cutoff
    forecast['category_guid']=category_guid
    return forecast




def check_having_cross_id(cross_id,forecast_id,lag,future_date,rrc_guid=''):
    #if certain_cross_id=='':
    #    return False
    if rrc_guid!='':
        scr_check=f"""
            select cross_id from forecast.cross_validation_torrc_full
            where cross_id={cross_id}
                and forecast_id={forecast_id}
                and lag={lag}
                and ds='{future_date}'
                and rrc_guid='{rrc_guid}'
            limit 1
        """
        df=df_build('delta_bi',scr_check)
        return (not df.empty) # если этот набор модель/лаг/дата/ррц уже есть - пропускаем
    elif rrc_guid=='':
        scr_check=f"""
            select cross_id from forecast.cross_validation_full
            where cross_id={cross_id}
                and forecast_id={forecast_id}
                and lag={lag}
                and ds='{future_date}'
            limit 1
        """
        df=df_build('delta_bi',scr_check)
        return (not df.empty) # если этот набор модель/лаг/дата уже есть - пропускаем
        
        

def cross_best_models_rrc(data,category_guid,future_date,lag):
    print(category_guid)
    # условная дата запуска
    cutoff=future_date-datetime.timedelta(weeks=lag)
    sales_d=data['sales_d']
    calendar=data['calendar']
    skiped_periods=data['skiped_periods']
    # список моделей для данной категории - по ухудшению (увеличению) метрики
    ranked_models=data['ranked_models']
    cycle=ranked_models.query(f"category_guid=='{category_guid}'").sort_values('weighted_value',ascending=True)
    
    # вычисляем заранее, т.к. нужно для месячного прогноза
    weekly,monthly=f_auto.create_weekly_and_monthly(data,category_guid,cutoff)
    data['weekly']=weekly
    data['monthly']=monthly
    
    # нужно для прогноза себестоимости и выручки
    sales28=create_days28(sales_d,calendar,skiped_periods,category_guid,cutoff)
    
    sales_d_rrc_product=download_data_rrc(category_guid,full=False)
    data['sales_d_rrc_product']=sales_d_rrc_product
    for forecast_id in cycle.forecast_id:
        print('перебираем прогнозы')
        if 'temp' in dir():
            del temp
        if forecast_id==9:# новый профет
            temp=f_prophet.main(data,category_guid,cutoff)
        elif forecast_id==10: # автопрогноз
            temp=f_auto.main(data,category_guid,cutoff)
        
        if temp.y_forecast.sum()>0 and temp.shape[0]==105:
            #to_csv(temp,'forecast'+category_guid)
            temp=temp[temp.ds==future_date]
            temp['forecast_id']=forecast_id
            temp['category_guid']=category_guid
            print('разбиваем до РРЦ')
            temp_result=break_to_rrc_product(temp,data,category_guid,cutoff,level=1)
            #to_csv(temp_result,'torrc'+category_guid)
            temp_result['forecast_id']=forecast_id
            #temp_result['cross_id']=cross_id
            temp_result['lag']=lag
            

    return temp_result

def check_lags(lags):
    if sum([lag<0 for lag in lags])>0:
        raise Exception('В списке лагов есть отрицательные числа. Работа остановлена')

def check_futures(calendar,futures):
    #futures=['2022-12-12','2022-12-05','2022-11-28','2022-11-21']
    #[2023-01-16, 2023-01-09, 2022-12-12, 2022-12-05]
    # futures одна дата - дата запуска. список - собственно список прогнозируемых дат
    # если пустые прогнозные даты/дата запуска - ближайший понедельник слева. типа, дата запуска
    if futures=='':
        futures=calendar[calendar.ds<datetime.datetime.now()].query('wday==1').sort_values('ds',ascending=False).ds.head(1).iloc[0]
    
    # если задали дату запуска - берём четыре предыдущих понедельника
    if not isinstance(futures,list):
        # условие на недели - из заявки https://sdms.dns-shop.ru/app#e1cib/data/Документ.Задача?ref=a24400155dfc825611ed87536cb769e0
        futures=calendar[calendar.ds<futures].query('wday==1 and week_mon>1 and week_mon<51')\
                    .sort_values('ds',ascending=False).ds.head(4).apply(lambda x:str(x)[:10]).to_list()
                    
    # в списке дат прогнозирования должны быть только понедельники. иначе выдаём ошибку
    futures_df=pd.DataFrame(futures,columns=['ds'])
    futures_df.ds=futures_df.ds.astype('datetime64[ns]')
    if not futures_df.merge(calendar).query('wday!=1').empty:
        raise Exception('В списке дат запуска есть не понедельники. Работа остановлена')

    return futures


def create_svod(result,sales_w,cols=['ds','category_guid'],rrc_guid=''):
    """
    сбор данных из параллельных вычислений
    подтягивание реальных продаж для вычисления метрик
    вход:
        result (список из датафреймов) - прогнозы из параллельных вычислений
        sales_w (датафрейм) - продажи неделя-категория
    выход:
        svod (датафрейм) - прогнозы неделя-категория + реальные продажи
    """
    # собираем прогнозы (созданные параллельно) в датасет
    
    svod=pd.concat([data for data in result],axis=0)
    svod.ds=svod.ds.astype('datetime64[ns]')
    svod.y_forecast=svod.y_forecast.fillna(0)
    
    try:
        svod=svod.drop(columns='y')
    except:
        pass
        
    if rrc_guid!='':
        svod['rrc_guid']=rrc_guid
    # подтягиваем реальные продажи
    svod=svod.merge(sales_w,how='left',on=cols)
    svod.y=svod.y.fillna(0)
    
    return svod

# вводим метрики
def dif(df):
    # чтобы подстроить под разные названия столбцов и не вводить промежуточные
    return abs(df.y_forecast-df.y)

def mae(df):
    """
    Метрика Mean Absolute Error - средняя абсолютная ошибка
    """
    return dif(df).mean()

def mape(df):
    """
    Метрика Mean Absolute Percentage Error - средняя абсолютная процентная ошибка
    """
    df['temp']=abs(dif(df)/df.y)
    df.loc[df.y==0,'temp']=0
    #print(temp)
    if sum(df.temp)>0:
        #return abs(dif(df)/df.y).mean()
        return df.temp.mean()
    else:
        return 0

def wape(df):
    """
    Метрика Weighted Absolute Percentage Error - взвешенная абсолютная ошибка в процентах
    """
    if sum(df.y)>0:
        return sum(dif(df))/sum(df.y)
    else:
        return 0   

def create_metrics(svod,cols=['category_guid','lag']):   
    """
    создаёт метрики для связок категория-лаг
    вход:
        svod (датафрейм) - прогнозы неделя-категория + реальные продажи
    выход:
        metrics (датафрейм) - метрики в разрезе категория - лаг
    """

    for func in [mae,mape,wape]:
        temp=svod.groupby(cols).apply(func).reset_index()
        temp['metric']=func.__name__
        temp=temp.rename(columns={0:'value'})
        
        if 'metrics' not in locals():
            metrics=temp.copy()
        else:
            metrics=metrics.append(temp)
    #svod.value=svod.value.fillna(0)
    return metrics    

def upload_data(svod,metrics,cross_id,forecast_id,is_test=''):
    """
    записываем данные в базу, отправляем сообщение в скайп-лог
    вход:
        svod (датафрейм) - прогнозы неделя-категория + реальные продажи
        metrics (датафрейм) - метрики в разрезе категория - лаг
        cross_id - ИД кросс-валидации (счётчик)
        forecast_id - ИД прогноза (из справочника прогнозов)
    выход:
        -
    """
    if svod is not None:
        svod['cross_id']=cross_id
        svod['forecast_id']=forecast_id
        svod['execute_dt']=datetime.datetime.now(tz)
        df_load('delta_bi','forecast.cross_validation_full',svod)
    
    if metrics is not None:
        if cross_id is not None:
            metrics['cross_id']=cross_id
        if forecast_id is not None:
            metrics['forecast_id']=forecast_id
        df_load('delta_bi','forecast.cross_validation_accuracy',metrics)
    
    #forecast_name=df_build('delta_bi',f'select * from forecast.dim_forecasts where forecast_id={forecast_id}').name_1c[0]
    
    #if is_test==0:
    #    toSkype(datetime.datetime.now(pytz.timezone('Asia/Vladivostok')).strftime('%Y-%m-%d %H:%M:%S')+' влк\n'+\
    #            'Прогноз продаж, кросс-валидация'+'\n\n'+\
    #            f'{forecast_name} - в базу загружено {metrics.shape[0]} строк по {metrics.category_guid.nunique()} категориям',chat='Прогнозы')

def upload_data_rrc(svod,metrics,cross_id,forecast_id,is_test):
    """
    записываем данные в базу, отправляем сообщение в скайп-лог
    вход:
        svod (датафрейм) - прогнозы неделя-категория + реальные продажи
        metrics (датафрейм) - метрики в разрезе категория - лаг
        cross_id - ИД кросс-валидации (счётчик)
        forecast_id - ИД прогноза (из справочника прогнозов)
    выход:
        -
    """
    #svod['cross_id']=cross_id
    #svod['forecast_id']=forecast_id
    #df_load('delta_bi','forecast.cross_validation_torrc_full',svod)
            
    metrics['cross_id']=cross_id
    if forecast_id!=0:
        forecast_name=df_build('delta_bi',f'select * from forecast.dim_forecasts where forecast_id={forecast_id}').name_1c[0]
        metrics['forecast_id']=forecast_id
    df_load('delta_bi','forecast.cross_validation_torrc_accuracy',metrics)
    
    
    
    if is_test==0:
        cnt=metrics[['rrc_guid','category_guid']].drop_duplicates().shape[0]
        toSkype(datetime.datetime.now(pytz.timezone('Asia/Vladivostok')).strftime('%Y-%m-%d %H:%M:%S')+' влк\n'+\
                'Прогноз продаж, кросс-валидация категория-РРЦ'+'\n\n'+\
                f'{forecast_name} - в базу загружено {metrics.shape[0]} строк по {cnt} категориям-РРЦ',chat='Прогнозы')
                
def main_stable(lags,futures,is_test=0):
    """
    процедура запуска. обрабатывает данные, формирует прогноз, загружает в базу
    вход:
        lags (список) - прогнозы неделя-категория + реальные продажи
        futures (список / дата) - даты, которые нужно спрогнозировать / дата запуска, от которой надо взять четыре предыдущих понедельника
        is_test (1/0) - является ли кросс-валидация тестовой
    выход:
        -
    """
    if sum([lag<0 for lag in lags])>0:
        raise Exception('В списке лагов есть отрицательные числа. Работа остановлена')

    # выгружаем данные
    data=download_data()
    cats,calendar,sales_d,sales_w,skiped_periods=data['cats'],data['calendar'],data['sales_d'],data['sales_w'],data['skiped_periods']
    
    
    # если пустые прогнозные даты/дата запуска - ближайший первый понедельник месяца слева. типа, дата запуска
    if futures=='':
        futures=calendar[calendar.ds<datetime.datetime.now()].query('mday==1').sort_values('ds',ascending=False).ds.head(1).iloc[0]
    
    # если задали дату запуска - берём четыре предыдущих понедельника
    if not isinstance(futures,list):
        futures=calendar[calendar.ds<futures].query('wday==1')\
                    .sort_values('ds',ascending=False).ds.head(4).apply(lambda x:str(x)[:10]).to_list()
    
    # вычисляем ИД кросс-валидации
    cross_id=df_build('delta_bi','select coalesce(max(cross_id),0) id from forecast.fact_cross')
    cross_id=cross_id.id[0]+1
    
    # в списке дат прогнозирования должны быть только понедельники. иначе выдаём ошибку
    futures_df=pd.DataFrame(futures,columns=['ds'])
    futures_df.ds=futures_df.ds.astype('datetime64[ns]')
    if not futures_df.merge(calendar).query('wday!=1').empty:
        raise Exception('В списке дат запуска есть не понедельники. Работа остановлена')
    
    # записываем данные о запуске кросс-валидации
    df_load('delta_bi','forecast.fact_cross',{'cross_id':cross_id,'dt':datetime.datetime.now(tz),'lags':str(lags),'futures':str(futures),'is_test':1})
    
    cats=cats[::40]

    print('перебираем лаги и даты для автопрогноза')
    toSkype('кросс-валидация, начинаем автопрогноз','Airflow')
    #result=[]
    for lag in lags:
        toSkype(f'кросс-валидация, автопрогноз, {lag}','Airflow')
        result=[]
        for future in futures:
            
            x=datetime.datetime.now()
            
            future_date=datetime.datetime.strptime(future,'%Y-%m-%d')
            cutoff=future_date-datetime.timedelta(weeks=lag)

            sales_d_consistent=f_auto.create_consistency_sales(data,report_dt=cutoff)
            data['sales_d_consistent']=sales_d_consistent
            
            #temp_result=Parallel(n_jobs=8)(delayed(cross_autoforecast)(data,category_guid,future_date,lag)
            #                                   for category_guid in cats.category_guid)
            for category_guid in cats.category_guid:
                temp_result=cross_autoforecast(data,category_guid,future_date,lag)
                result.append(temp_result)
            #result.extend(temp_result)
            y=datetime.datetime.now()
            print(future,lag,str(y-x))
    

        svod=create_svod(result,sales_w)
        metrics=create_metrics(svod) 
        # записываем в базу разбивку прогнозов и метрики
        upload_data(svod,metrics,cross_id,10,is_test)

    
    print('перебираем лаги и даты для профета')
    toSkype('кросс-валидация, начинаем профет','Airflow')
    
    for lag in lags:
        result=[]
        for future in futures:
            x=datetime.datetime.now()
            
            future_date=datetime.datetime.strptime(future,'%Y-%m-%d')
            temp_result=Parallel(n_jobs=5)(delayed(cross_prophet)(data,category_guid,future_date,lag)
                                               for category_guid in cats.category_guid)
            result.extend(temp_result)
            y=datetime.datetime.now()
            print(future,lag,str(y-x))
            
        svod=create_svod(result,sales_w)
        metrics=create_metrics(svod) 
        # записываем в базу разбивку прогнозов и метрики
        upload_data(svod,metrics,cross_id,9,is_test)
    
    
    print('перебираем лаги и даты для Автопрогноз_53_3нТренд')
    
    for lag in lags:
        result=[]
        for future in futures:
            x=datetime.datetime.now()
            
            future_date=datetime.datetime.strptime(future,'%Y-%m-%d')
            temp_result=Parallel(n_jobs=5)(delayed(cross_trend1)(data,category_guid,future_date,lag)
                                               for category_guid in cats.category_guid)
            result.extend(temp_result)
            y=datetime.datetime.now()
            print(future,lag,str(y-x))
            
        svod=create_svod(result,sales_w)
        metrics=create_metrics(svod) 
        # записываем в базу разбивку прогнозов и метрики
        upload_data(svod,metrics,cross_id,12,is_test)
    
    
    print('перебираем лаги и даты для Автопрогноз_53_2_52_3нТренд')
    
    for lag in lags:
        result=[]
        for future in futures:
            x=datetime.datetime.now()
            
            future_date=datetime.datetime.strptime(future,'%Y-%m-%d')
            temp_result=Parallel(n_jobs=5)(delayed(cross_trend2)(data,category_guid,future_date,lag)
                                               for category_guid in cats.category_guid)
            result.extend(temp_result)
            y=datetime.datetime.now()
            print(future,lag,str(y-x))
            
    svod=create_svod(result,sales_w)
    metrics=create_metrics(svod) 
    # записываем в базу разбивку прогнозов и метрики
    upload_data(svod,metrics,cross_id,13,is_test)
    
    
    print('перебираем лаги и даты для Автопрогноз_53_4_52_3нТренд')
    
    for lag in lags:
        result=[]
        for future in futures:
            x=datetime.datetime.now()
            
            future_date=datetime.datetime.strptime(future,'%Y-%m-%d')
            temp_result=Parallel(n_jobs=5)(delayed(cross_trend3)(data,category_guid,future_date,lag)
                                               for category_guid in cats.category_guid)
            result.extend(temp_result)
            y=datetime.datetime.now()
            print(future,lag,str(y-x))
            
    svod=create_svod(result,sales_w)
    metrics=create_metrics(svod) 
    # записываем в базу разбивку прогнозов и метрики
    upload_data(svod,metrics,cross_id,14,is_test)
    
    
    print('перебираем лаги и даты для АвтопрогнозФормулаНакопСумма')
    
    for lag in lags:
        result=[]
        for future in futures:
            x=datetime.datetime.now()
            
            future_date=datetime.datetime.strptime(future,'%Y-%m-%d')
            temp_result=Parallel(n_jobs=5)(delayed(cross_trend4)(data,category_guid,future_date,lag)
                                               for category_guid in cats.category_guid)
            result.extend(temp_result)
            y=datetime.datetime.now()
            print(future,lag,str(y-x))
            
    svod=create_svod(result,sales_w)
    metrics=create_metrics(svod) 
    # записываем в базу разбивку прогнозов и метрики
    upload_data(svod,metrics,cross_id,15,is_test)
    
    
    
    if is_test==0:
        df_build('delta_bi','select 1 cnt', f'update forecast.fact_cross set is_test=0 where cross_id={cross_id}')
        
def check_cross_id(certain_cross_id,tablename,lags,futures):
    # проверяем, есть ли у нас прервавшиеся кросс-валидации
    scr_check=f"""
        select cross_id,is_test from forecast.{tablename}
        order by cross_id desc
        limit 1
    """
    df=df_build('delta_bi',scr_check)
    
    
    if df.is_test.item()==777:
        certain_cross_id=df.cross_id.item()

    
    # вычисляем ИД кросс-валидации
    if certain_cross_id!='':
        cross_id=certain_cross_id
    else:
        cross_id=df_build('delta_bi',f'select coalesce(max(cross_id),0) id from forecast.{tablename}')
        cross_id=cross_id.id[0]+1
        
        # записываем данные о запуске кросс-валидации
        # 777 - что загружается. если оборвётся - по нему найдём
        df_load('delta_bi',f'forecast.{tablename}',{'cross_id':cross_id,'dt':datetime.datetime.now(tz),'lags':str(lags),'futures':str(futures),'is_test':777})
        
        
    return cross_id

        
def main_rrc_stable(lags,futures,is_test=0,certain_cross_id=''):
    """
    процедура запуска. обрабатывает данные, формирует прогноз, загружает в базу
    вход:
        lags (список) - прогнозы неделя-категория + реальные продажи
        futures (список / дата) - даты, которые нужно спрогнозировать / дата запуска, от которой надо взять четыре предыдущих понедельника
        is_test (1/0) - является ли кросс-валидация тестовой
    выход:
        -
    """
    if sum([lag<0 for lag in lags])>0:
        raise Exception('В списке лагов есть отрицательные числа. Работа остановлена')


    data_full=download_data(min_year=2019)
    
    sales_w_rrc=download_data_rrc('')

    
    cats=data_full['cats']
    calendar=data_full['calendar']
    sales_d=data_full['sales_d']
    skiped_periods=data_full['skiped_periods']

    # если пустые прогнозные даты/дата запуска - ближайший первый понедельник месяца слева. типа, дата запуска
    if futures=='':
        futures=calendar[calendar.ds<datetime.datetime.now()].query('mday==1').sort_values('ds',ascending=False).ds.head(1).iloc[0]
    
    # если задали дату запуска - берём четыре предыдущих понедельника
    if not isinstance(futures,list):
        futures=calendar[calendar.ds<futures].query('wday==1')\
                    .sort_values('ds',ascending=False).ds.head(4).apply(lambda x:str(x)[:10]).to_list()
    
    # в списке дат прогнозирования должны быть только понедельники. иначе выдаём ошибку
    futures_df=pd.DataFrame(futures,columns=['ds'])
    futures_df.ds=futures_df.ds.astype('datetime64[ns]')
    if not futures_df.merge(calendar).query('wday!=1').empty:
        raise Exception('В списке дат запуска есть не понедельники. Работа остановлена')
    




    cross_id=check_cross_id(certain_cross_id,'fact_cross_torrc',lags,futures)





    # для каждой категории сортируем модели по убыванию средневзвешенной метрики MAE
    ranked_models, xxx = create_ranked_models()
    data_full['ranked_models']=ranked_models
    
    
    #sales_d_rrc=download_data_rrc_d(data_full)
    #data['sales_d_rrc']=sales_d_rrc
    
    scr="""
    with main as 
    (
    select date date_day,min(id) over(partition by GFK_Week) date_id, min(date) over(partition by GFK_Week) ds
    from dbo_dim_date
    where date>='2018-01-01'
    and datetime=daybegindate--and date_day<='2019-03-31'
    )
    select distinct ds,date_id
    from main
    """
    
    #ds_dict=df_build('dns_dwh',scr)
    #ds_dict.ds=ds_dict.ds.astype('datetime64[ns]')
    #data_full['ds_dict']=ds_dict

    predictors=df_build('dns_dwh','forecast_sales_predictors_dayoff')
    data_full['predictors']=predictors

    
    
    
    
    logging.warning('начало автопрогноза '+str(datetime.datetime.now()))
    forecast_id=11

   
    data=data_full.copy() # чтобы не портить оригинальную sales_d
    for rrc_guid in sales_w_rrc.rrc_guid.drop_duplicates().sort_values():
    
        print(rrc_guid)
        toSkype(str(datetime.datetime.now())+' автопрогноз '+rrc_guid,'Airflow')
        
        if 'svod' in dir():
            del svod
        data={}
        data['calendar']=calendar
        data['skiped_periods']=skiped_periods
        data['year_exclusion']=data_full['year_exclusion']
                
        # data_full - ради справочника групп категорий
        sales_d=download_data_rrc_d(data_full,rrc_guid)
        data['sales_d']=sales_d            
        
        for lag in lags:
            
            future_date=datetime.datetime.strptime(futures[0],'%Y-%m-%d')
            cutoff=future_date-datetime.timedelta(weeks=lag)

            if check_having_cross_id(cross_id,forecast_id,lag,future_date,rrc_guid): # если этот набор лаг/дата/ррц уже есть - пропускаем
                continue

            #toSkype(str(datetime.datetime.now())+' отфильтровали продажи, начинаем consistent','Airflow')
            data['sales_d_consistent']=f_auto.create_consistency_sales(data,cutoff) # для каждого РРЦ и даты отдельно
            sales_d_consistent=data['sales_d_consistent']
            #sales_w_rrc=data['sales_w_rrc'].query(f'rrc_guid=="{rrc_guid}"')

            #toSkype(str(datetime.datetime.now())+' начинаем цикл категорий автопрогноз','Airflow')
            
            for category_guid in cats.category_guid:
                forecast=cross_autoforecast_rrc(data,category_guid,futures,lag)
                if 'svod' in dir():
                    svod=pd.concat([svod,forecast],axis=0)
                else:
                    svod=forecast.copy()
                    
            #toSkype(str(datetime.datetime.now())+' законч цикл категорий','Airflow')      
            
            
            #toSkype(str(datetime.datetime.now())+' начинаем паралл категорий автопрогноз','Airflow')
            #result=Parallel(n_jobs=10)(delayed(cross_autoforecast)(data,category_guid,future_date,lag)
            #            for category_guid in cats.category_guid[::40]) #in ['fd72100d-70e6-11e2-b24e-00155d030b1f',])#
            #toSkype(str(datetime.datetime.now())+' законч паралл категорий автопрогноз','Airflow')  
           

            #svod=create_svod(result,sales_w_rrc,cols=['ds','category_guid','rrc_guid'])

            #svod=pd.concat([data for data in result],axis=0) # для теста параллельности
            
            #print(datetime.datetime.now())#4
            try:
                svod=svod.drop(columns='y')
            except:
                pass
        to_csv(svod,'svod')         
        x=input()
        svod.ds=svod.ds.astype('datetime64[ns]')        
        svod['cross_id']=cross_id
        svod['rrc_guid']=rrc_guid
        svod['forecast_id']=forecast_id
        svod['execute_dt']=datetime.datetime.now(tz)

        svod=svod.merge(sales_w_rrc,how='left',on=['ds','category_guid','rrc_guid'])
        svod.y_forecast=svod.y_forecast.fillna(0)
        svod.y=svod.y.fillna(0)

        #print(datetime.datetime.now())#6
        df_load('delta_bi','forecast.cross_validation_torrc_full',svod)                                   

                
    svod=df_build('delta_bi',f'select * from forecast.cross_validation_torrc_full where cross_id={cross_id} and forecast_id = {forecast_id}')  
    if 'metrics' in dir():
        del metrics        
    metrics=create_metrics(svod,cols=['rrc_guid','category_guid','lag','forecast_id']) 
    # записываем в базу разбивку прогнозов и метрики
    upload_data_rrc(svod,metrics,cross_id,forecast_id,is_test=1)



    
    
    
    
    
    
    
    
    
    logging.warning('начало профета-РРЦ'+str(datetime.datetime.now()))

    print('перебираем лаги и даты для профета РРЦ-категория')
    forecast_id=8    
    for rrc_guid in sales_w_rrc.rrc_guid.drop_duplicates().sort_values():
        toSkype(str(datetime.datetime.now())+' профет '+rrc_guid,'Airflow')
        #data=data_full.copy()
        #sales_d=download_data_rrc_d(data,rrc_guid)
        #data['sales_d']=sales_d
        #data['sales_w']=sales_w_rrc.query(f'rrc_guid=="{rrc_guid}"')
        data={}
        data['calendar']=calendar
        data['sales_w']=sales_w_rrc.query(f'rrc_guid=="{rrc_guid}"')
        

        for lag in lags:
            future_date=datetime.datetime.strptime(futures[0],'%Y-%m-%d')
            if check_having_cross_id(cross_id,forecast_id,lag,future_date,rrc_guid): # если этот набор лаг/дата/ррц уже есть - пропускаем
                continue
               
            #toSkype(str(datetime.datetime.now())+' начинаем паралл категорий профет','Airflow')
            #result=Parallel(n_jobs=10)(delayed(cross_prophet)(data,category_guid,future_date,lag)#data,category_guid,future_date,lag
            #                       for category_guid in cats.category_guid[::40])#['fd72100d-70e6-11e2-b24e-00155d030b1f',])#
            result=Parallel(n_jobs=5)(delayed(cross_prophet_rrc)(data,category_guid,futures,lag)#data,category_guid,future_date,lag
                                   for category_guid in cats.category_guid)#['fd72100d-70e6-11e2-b24e-00155d030b1f',])#
            #toSkype(str(datetime.datetime.now())+' закончили паралл категорий профет','Airflow')
                    
            svod=create_svod(result,data['sales_w'],cols=['ds','category_guid','rrc_guid'],rrc_guid=rrc_guid)
             
            svod['cross_id']=cross_id
            svod['forecast_id']=forecast_id
            svod['execute_dt']=datetime.datetime.now(tz)
            df_load('delta_bi','forecast.cross_validation_torrc_full',svod)
    
    svod=df_build('delta_bi',f'select * from forecast.cross_validation_torrc_full where cross_id={cross_id} and forecast_id={forecast_id}')  
    if 'metrics' in dir():
        del metrics        
    metrics=create_metrics(svod,cols=['rrc_guid','category_guid','lag','forecast_id']) 
    # записываем в базу разбивку прогнозов и метрики
    upload_data_rrc(svod,metrics,cross_id,forecast_id,is_test=1)
    
    logging.warning('конец профета-РРЦ '+str(datetime.datetime.now()))
    
    

    df_build('delta_bi','select 1 cnt', f'update forecast.fact_cross_torrc set is_test={is_test} where cross_id={cross_id}')
    


def cross_cross(data,cats,forecast_id,cross_id,lag,futures):
    toSkype(str(forecast_id)+' '+str(lag),'A')
    sales_w=data['sales_w']
    
    cross_function_str=data['plantype'].query(f'forecast_id=={forecast_id}').cross_function.item()
    cross_function=eval(cross_function_str)
    
    for future in futures:
        result=[]
        future_date=datetime.datetime.strptime(future,'%Y-%m-%d')
        
        x=datetime.datetime.now()
        
        if check_having_cross_id(cross_id,forecast_id,lag,future_date): # если этот набор модель/лаг/дата уже есть - пропускаем
            continue
                
        if cross_function_str in ['cross_autoforecast','cross_autoforecast_rrc']:
            cutoff=future_date-datetime.timedelta(weeks=lag)
            sales_d_consistent=f_auto.create_consistency_sales(data,report_dt=cutoff)
            data['sales_d_consistent']=sales_d_consistent
            
        for category_guid in cats.category_guid:
            temp_result=cross_function(data,category_guid,future_date,lag)
            result.append(temp_result)
            
        if len(result)>0:
            svod=create_svod(result,sales_w)
            # записываем в базу разбивку прогнозов
            svod['cross_id']=cross_id
            svod['forecast_id']=forecast_id
            svod['execute_dt']=datetime.datetime.now(tz)
            df_load('delta_bi','forecast.cross_validation_full',svod)
            
            # предосторожность, если процесс прервётся на середине заливки какой-то связки
            # и она зальётся не полностью
            # строки с пустым столбцом is_full удаляем при проверки cross_id
            scr_update=f"""
            update forecast.cross_validation_full
            set is_full=1
            where cross_id={cross_id}
                and forecast_id={forecast_id}
                and lag={lag}
                and ds='{future}'
            """
            df_build('delta_bi','select 1 cnt',scr_update)
            
        y=datetime.datetime.now()
        toSkype('Кросс-валидация до категории '+cross_function_str+' '+str(lag)+' '+future+' '+str(y-x),'Airflow')
    


def cross_cross_rrc(data,cats,forecasts,cross_id,lags,futures,rrc_guid):
    #toSkype('начали РРЦ '+rrc_guid,'Airflow')
    sales_w=download_data_rrc('',rrc_guid=rrc_guid)
    data['sales_w']=sales_w
    
    sales_d=download_data_rrc_d(data,rrc_guid=rrc_guid)
    data['sales_d']=sales_d
    
    
    for forecast_id in forecasts:
        cross_function_str=data['plantype'].query(f'forecast_id=={forecast_id}').cross_function.item()
        cross_function=eval(cross_function_str)
        
        
        for lag in lags:
            toSkype('начали РРЦ '+rrc_guid+' '+str(lag)+' '+cross_function_str,'Airflow')
            result=[]
            future_date=datetime.datetime.strptime(futures[0],'%Y-%m-%d')
            
            x=datetime.datetime.now()
            
            if check_having_cross_id(cross_id,forecast_id,lag,future_date,rrc_guid): # если этот набор модель/лаг/дата/ррц уже есть - пропускаем
                return ''
                    
            if cross_function_str in ['cross_autoforecast','cross_autoforecast_rrc']:
                cutoff=future_date-datetime.timedelta(weeks=lag)
                sales_d_consistent=f_auto.create_consistency_sales(data,report_dt=cutoff)
                data['sales_d_consistent']=sales_d_consistent
                
            for category_guid in cats.category_guid:
                temp_result=cross_function(data,category_guid,futures,lag)
                result.append(temp_result)
                
            if len(result)>0:
                svod=create_svod(result,sales_w)
                # записываем в базу разбивку прогнозов
                svod['cross_id']=cross_id
                svod['forecast_id']=forecast_id
                svod['rrc_guid']=rrc_guid
                svod['execute_dt']=datetime.datetime.now(tz)
                df_load('delta_bi','forecast.cross_validation_torrc_full',svod)
                
                # предосторожность, если процесс прервётся на середине заливки какой-то связки
                # и она зальётся не полностью
                # строки с пустым столбцом is_full удаляем при проверки cross_id
                scr_update=f"""
                update forecast.cross_validation_torrc_full
                set is_full=1
                where cross_id={cross_id}
                    and forecast_id={forecast_id}
                    and lag={lag}
                    and rrc_guid='{rrc_guid}'
                """
                df_build('delta_bi','select 1 cnt',scr_update)
                
            y=datetime.datetime.now()
            #toSkype(cross_function_str+' '+str(lag)+' '+futures[0]+' '+rrc_guid+' '+str(y-x),'Airflow')
    
def main(lags,futures,is_test=0,certain_cross_id=''):
    """
    процедура запуска. обрабатывает данные, формирует прогноз, загружает в базу
    вход:
        lags (список) - прогнозы неделя-категория + реальные продажи
        futures (список / дата) - даты, которые нужно спрогнозировать / дата запуска, от которой надо взять четыре предыдущих понедельника
        is_test (1/0) - является ли кросс-валидация тестовой
    выход:
        -
    """
    try:
        toSkype('кросс-валидация до категории\n'+'самое начало',chat='Airflow')
        check_lags(lags)
        
        # выгружаем данные
        data=download_data()
        cats,calendar=data['cats'],data['calendar']
        
        #cats=cats.query('category1_number==8')
        
        futures=check_futures(calendar,futures)

        cross_id=check_cross_id(certain_cross_id,'fact_cross',lags,futures)
     
        scr_delete_half_uploaded=f"""
            delete
            from forecast.cross_validation_full
            where cross_id={cross_id}
                and is_full is NULL
        """
        df_build('delta_bi','select 1 cnt',scr_delete_half_uploaded)
     
        #cats=cats[::80]
        #cats=cats.query('category_guid in ("fd72104a-70e6-11e2-b24e-00155d030b1f","fd721058-70e6-11e2-b24e-00155d030b1f","8b8877a6-718e-11e2-b24e-00155d030b1f","842d4a41-9fa5-11e4-a83d-00155d03361b","7dfcb376-f036-11e3-aea1-00155d031202","0882e52a-796e-11e2-b7ec-00155d030b1f","2dfe67f9-8118-11ec-8f69-00155d8ed20b","fd721047-70e6-11e2-b24e-00155d030b1f")')
        toSkype('кросс-валидация до категории\n'+'начинаем параллель',chat='Airflow')
        # первым идёт модель, чтобы длинный профет сразу прогрузился, и потом не остался без параллельных.
        # типа, сначала кладём в банку большие камни, потом мелкие
        
        
        #del data['sales_d']
        Parallel(n_jobs=8)(delayed(cross_cross)(data,cats,forecast_id,cross_id,lag,futures)
                                     for forecast_id in [9,10,12,13,14,15] for lag in lags)
        
        # грузим метрики одним махом
        scr_svod=f"""
            select *
            from forecast.cross_validation_full
            where cross_id={cross_id}
            """
        svod=df_build('delta_bi',scr_svod)
        metrics=create_metrics(svod,cols=['category_guid','lag','cross_id','forecast_id']) 
        # записываем в базу метрики
        df_load('delta_bi','forecast.cross_validation_accuracy',metrics)
                
        df_build('delta_bi','select 1 cnt', f'update forecast.fact_cross set is_test={is_test} where cross_id={cross_id}')
        
        if is_test==0:
            toSkype(datetime.datetime.now(pytz.timezone('Asia/Vladivostok')).strftime('%Y-%m-%d %H:%M:%S')+' влк\n'+\
                'Планы - кросс-валидация завершена',chat='Прогнозы')
    except:
        logging.warning(str(traceback.format_exc()))
        toSkype('кросс-валидация до категории\n'+str(traceback.format_exc()),chat='Airflow')
    
    
def main_rrc(lags,futures,is_test=0,certain_cross_id=''):
    """
    процедура запуска. обрабатывает данные, формирует прогноз, загружает в базу
    вход:
        lags (список) - прогнозы неделя-категория + реальные продажи
        futures (список / дата) - даты, которые нужно спрогнозировать / дата запуска, от которой надо взять четыре предыдущих понедельника
        is_test (1/0) - является ли кросс-валидация тестовой
    выход:
        -
    """
    
    check_lags(lags)
    
    # выгружаем данные
    data=download_data()
    
    # наполняем в параллельности
    del data['sales_w']
    del data['sales_d']
    
    cats=data['cats']
    calendar=data['calendar']
    skiped_periods=data['skiped_periods']
    storehouse_actual=data['storehouse_actual']
    
    futures=check_futures(calendar,futures)

    cross_id=check_cross_id(certain_cross_id,'fact_cross_torrc',lags,futures)
 
    scr_delete_half_uploaded=f"""
        delete
        from forecast.cross_validation_torrc_full
        where cross_id={cross_id}
            and is_full is NULL
    """
    df_build('delta_bi','select 1 cnt',scr_delete_half_uploaded)
 
    #cats=cats[::80]
    unique_stores= storehouse_actual.rrc_guid.sort_values()
    
    # первым идёт модель, чтобы длинный профет сразу прогрузился, и потом не остался без параллельных
    # типа, сначала кладём в банку большие камни, потом мелкие
    forecasts=[8,11]
    
    
    Parallel(n_jobs=5)(delayed(cross_cross_rrc)(data,cats,forecasts,cross_id,lags,futures,rrc_guid)
                                 for rrc_guid in unique_stores)
    
    # грузим метрики одним махом
    scr_svod=f"""
        select *
        from forecast.cross_validation_torrc_full
        where cross_id={cross_id}
        """
    svod=df_build('delta_bi',scr_svod)
    metrics=create_metrics(svod,cols=['rrc_guid','category_guid','lag','cross_id','forecast_id']) 
    # записываем в базу метрики
    df_load('delta_bi','forecast.cross_validation_torrc_accuracy',metrics)
            
    df_build('delta_bi','select 1 cnt', f'update forecast.fact_cross_torrc set is_test={is_test} where cross_id={cross_id}')
    
    
if __name__ == '__main__':
        main()