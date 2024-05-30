# подгружаем библиотеки
from my_functions import df_build,to_csv

from forecast_functions import compute_first_forecast_dt

import datetime
import pytz
tz = pytz.timezone('Asia/Irkutsk')

import pandas as pd
# необходимые модули библиотеки-предсказателя
import pystan
from prophet import Prophet



def delete_less_year_categories(sales,report_dt):
    """
    убираем категории, по которым доступно меньше года данных
    вход:
        sales (датафрейм) - продажи неделя-категория
        report_dt - дата запуска
    выход:
        sales (датафрейм) - продажи неделя-категория. по категориям, у которых больше года продаж
    """
    temp=sales.groupby(['category_guid']).agg({'ds':['min','max']}).reset_index()
    temp.columns=['category_guid','min_ds','max_ds']
    oneyear_cats=temp[((temp.min_ds<=report_dt-datetime.timedelta(days=365))&(temp.report_dt>=dt_fact-datetime.timedelta(days=28)))]
    sales=sales.merge(oneyear_cats,how='inner',on=['category_guid'])
    
    return sales.drop(columns=['min_ds','max_ds'])

def create_decart_sales(sales,first_forecast_dt,horizon_weeks):
    """
    создаём полный набор продаж (пропущенные недели - нули)
    вход:
        sales (датафрейм) - продажи неделя-категория
        first_forecast_dt - первая прогнозная дата
        to_forecast (датафрейм) - набор будущих дат для прогнозирования
    выход:
        decart_sales (датафрейм) - продажи неделя-категория. пропущенные недели заполнены нулями
    """
    
    # формируем набор дат, на которые будем делать прогноз. horizon_weeks недель
    to_forecast=pd.date_range(first_forecast_dt,first_forecast_dt+datetime.timedelta(weeks=horizon_weeks-1),periods=horizon_weeks)
    to_forecast=pd.DataFrame(to_forecast,columns=['ds'])
    to_forecast.ds=to_forecast.ds.astype('object')
    
    # формируем набор недель (с продажами и прогнозных)
    ds=sales[sales.ds<first_forecast_dt][['ds']].drop_duplicates()
    ds=ds.append(to_forecast)
    ds.ds=ds.ds.astype('datetime64[ns]')
    
    # уникальные категории в рамках датафрейма продаж
    categories=sales[['category_guid']].drop_duplicates()
    
    # все уникальные сочетания неделя-категория, подтягиваем к ним продажи, где не подтянулись - ноль
    decart_sales=ds.merge(categories,how='cross')
    decart_sales=decart_sales.merge(sales,how='left',on=['ds','category_guid'])
    decart_sales.y=decart_sales.y.fillna(0) 
    #decart_sales.ds=decart_sales.ds.astype('datetime64[ns]')

    return decart_sales

def create_forecast(decart_sales,category_guid,last_train_dt,first_forecast_dt,yearly_seasonality):
    """
    формируем модель по фиксированной категории
    вход:
        decart_sales (датафрейм) - декартовы продажи, т.е. пропуски в связках категория-неделя заменены нулями
        category_guid - категория
        first_forecast_dt - первая прогнозная дата
        last_train_dt - последняя дата для обучения модели
    выход:
        forecast (датафрейм) - прогноз на будущие даты
    """
    # фиксируем и категорию
    temp=decart_sales.query(f"category_guid=='{category_guid}'").fillna(0)
    
    # собираем нужный набор столбцов - неделя, объём
    temp=temp[['ds','y']]

    # обучаем только на фактах
    train_df=temp[temp.ds<=last_train_dt]
    m = Prophet(yearly_seasonality = yearly_seasonality, weekly_seasonality = False)
    try:
        m.fit(train_df)
    except: # Dataframe has less than 2 non-NaN rows
        return pd.DataFrame(columns=['ds','y_forecast','yhat_lower','yhat_upper'])
    
    # предсказываем 
    forecast = m.predict(temp)
    forecast=forecast[['ds','yhat','yhat_lower','yhat_upper']]
    
    forecast['category_guid']=category_guid
    forecast=forecast.rename(columns={'yhat':'y_forecast'})
    to_csv(forecast)
    # отрицательные прогнозы заменяем на нули
    forecast.loc[forecast.y_forecast<0,'y_forecast']=0

    return forecast[forecast.ds>=first_forecast_dt][['ds','y_forecast']]


def main(data,category_guid,report_dt='',yearly_seasonality=15,horizon_weeks=105):
    """
    процедура запуска. обрабатывает данные, формирует прогноз
    вход:
        sales_w (датафрейм) - продажи в разрезе неделя-категория
        calendar (датафрейм) - календарь
        category_guid - категория
        report_dt - дата запуска (т.к. иногда нужно запускать задним числом)
    выход:
        forecast (датафрейм) - прогноз
    """
    
    sales_w=data['sales_w']
    calendar=data['calendar']
    
    # если дата запуска не выбрана, то берём максимальную дату продаж
    if report_dt=='':
        report_dt=sales_w.ds.max()

    # убираем категории, по которым доступно меньше года данных
    # пока условие на полный год продаж убираем
    #sales_w=delete_less_year_categories(sales_w,report_dt)

    # вычисляем даты - первую прогнозную и последнюю обучающую
    first_forecast_dt,last_train_dt=compute_first_forecast_dt(calendar,report_dt)

    # создаём полный набор продаж (пропущенные недели - нули)
    decart_sales=create_decart_sales(sales_w,first_forecast_dt,horizon_weeks)
    
    # собственно, прогноз
    forecast=create_forecast(decart_sales,category_guid,last_train_dt,first_forecast_dt,yearly_seasonality)
    
    return forecast