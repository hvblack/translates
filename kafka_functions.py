"""
содержит функции, необходимые для передачи данных в Кафку
"""
import datetime
import json
import sys
from time import sleep

import pytz
tz = pytz.timezone('Asia/Irkutsk')

import pandas as pd

try:
    from ctypes import *
    CDLL(r"C:\Users\Khramenkov.VV\Anaconda3\Lib\site-packages\confluent_kafka.libs\librdkafka-a2007a74.DLL")
except:
    pass # для Airflow, где не нужно

# Для работы с Kafka 
from confluent_kafka import Producer

import numpy as np 
# подробная инфа про ошибки
import traceback
# Для работы с идентификаторами
import uuid

from my_functions import df_build, df_load, timemark, to_csv, timemark2,json_functions,recomm_column,toSkype,split_merge,df_load,to_sql

def new_recomm(main,recomm_type,to_null=False,task_uuid='',topic='ProductFixesByTypeRecommendationsNew',threshold=0.0001,merge_on=['branch_guid','product_guid'],value_col={'new_count':'Quantity'},database='dns_log',country='ru',check_null=False):
    """
    Создание датафрейма для передачи в kafka - 
    стыковка вычисленных рекомендаций с уже существующими. 
    Вход: 
        main(DataFrame) - датафрейм с новыми рекомендациями
        recomm_type(str) - guid рекомендации
        to_null (bool) - если необходимо массовое зануление ранее загруженных рекомендаций
        task_uuid (uuid) - гуид задачи (для логирования)
        topic (str) - название топика в Кафке
        threshold (float) - число, от которого мы считаем значения разными
        merge_on - разрез, по которому стыкуем новые и действующие рекомендации. В основном филиал-продукт, но бывают исключения
        value_col - словарь-сопоставление колонок: как называется у меня - как называется в регистре. Если заливаю несколько колонок - будет несколько пар в словаре
    ):
    Выход:
        (itog) - результат работы. 
    """
    # иногда стыкуем по гуиду рекомендации + дивизиону (при загрузке в региональные ИБД)
    # поэтому можем передать два значения в списке
    if topic=='ProductFixesByTypeRecommendations':
        topic='ProductFixesByTypeRecommendationsNew'
    if isinstance(recomm_type,dict):
        DivisionName=recomm_type['DivisionName']
        recomm_type=recomm_type['recomm_type']
    else:
        DivisionName=''

    if isinstance(database,list):
        database_conn=database[0]+('_'+country if country!='ru' else '')
        database_name=database[1]
    elif isinstance(database,str):
        database_conn=database+('_'+country if country!='ru' else '')
        database_name=database
    
    xxx="""
    category_exclusion_guids=df_build('delta_bi','select * from etl.category_exclusion_guids')
    if not category_exclusion_guids.query(f'guid=="{recomm_type}"').empty: # если есть в списке гуидов рекомендаций
        category_exclusion=df_build('delta_bi','select * from etl.category_exclusion')
        category_exclusion['ex_cat']=1
        if 'CategoryCode' in main.columns:
            pass
        elif 'CategoryID' in main.columns:
            cats=df_build('dns_dwh'+('_'+country if country!='ru' else ''),'categories_dnsdwh')
            main=main.merge(cats[['CategoryID','CategoryCode']],how='left',on='CategoryID')
        else:
            products=df_build('dns_dwh'+('_'+country if country!='ru' else ''),'products_dnsdwh')
            cats=df_build('dns_dwh'+('_'+country if country!='ru' else ''),'categories_dnsdwh')
            main=main.merge(products[['product_guid','CategoryID']],how='left',on='product_guid')
            main=main.merge(cats[['CategoryID','CategoryCode']],how='left',on='CategoryID')
            
        main=main.merge(category_exclusion,how='left',on='CategoryCode')
        main=main[main.ex_cat.isna()] # категория не в списке исключения
    """    
    #print(recomm_type,DivisionName)
    try:
    
        # логируем начало загрузки рекомендаций
        timemark2('log_new.txt',task_uuid,'Загрузка рекомендаций')
        
        # логируем общее колво рекомендаций
        #to_sql(main,'kafka_test_my')
        timemark2('log_new.txt',task_uuid,'Общее колво рекомендаций',str(main.query('new_count!=0').shape[0]))
            
        # скрипт актуальных рекомендаций по конкретному GUID
        recomm_script_ProductExposure_PrioritizationCoefficients=f""" -- текущие рекомендации
                with maxdate as
                (
                SELECT  main.BranchID, main.ProductID, max(main.DateOfRecording) dt
                FROM {database_name}.{topic} main
                    left join dns_retail.BranchHier BranchHier on main.BranchID = BranchHier.BranchGuid 
                where 1=1
                    and DivisionName ='{DivisionName}' -- дивизион
                group by main.BranchID, main.ProductID
                )
                ,
                main as
                (
                SELECT /*row_number(*) over(partition by main.ProductID,main.BranchID order by DateOfRecording desc) rank,*/ main.BranchID branch_guid,main.ProductID product_guid,Quantity -- берём существующие рекомендации, вычисляем ранг 
                FROM {database_name}.{topic} main
                    left join dns_retail.BranchHier BranchHier on main.BranchID = BranchHier.BranchGuid 
                    inner join maxdate on main.BranchID =maxdate.BranchID and main.ProductID =maxdate.ProductID
                where DivisionName ='{DivisionName}' -- дивизион
                    and main.DateOfRecording >=maxdate.dt
                order by DateOfRecording desc
                limit 1 by branch_guid,product_guid
                )
                select /*distinct*/ branch_guid,product_guid,Quantity
                from main
                where 1=1
                    and Quantity<>0
                union all
                select 'xxx','xxx',0 -- т.к. клик может выдать пустую таблицу вместо ошибки, скотина такая
                """
        recomm_script_ProductFixesByTypeRecommendations_old=f""" -- текущие рекомендации
                with main0 as
                (
                SELECT DateOfRecording, BranchID branch_guid,ProductID product_guid,Quantity,DeletionMark,Period,TypeRecommendationsID
                FROM {database_name}.{topic} main
                where 1=1
                    AND TypeRecommendationsID = '{recomm_type}'
                    and Period < (toDate(now()) + Interval '1 Day')
                )
                ,
                main1 as
                (
                SELECT row_number(*) over(partition by branch_guid,product_guid,TypeRecommendationsID, Period order by DateOfRecording desc) ra1,
                    branch_guid, product_guid,Quantity,DeletionMark,Period,TypeRecommendationsID
                FROM main0
                )
                ,
                main2 as 
                (
                select row_number(*) over(partition by branch_guid,product_guid,TypeRecommendationsID order by Period desc) ra2,branch_guid,product_guid,Quantity,Period
                from main1
                where 1=1
                    and ra1=1
                    and DeletionMark =0
                )
                select branch_guid,product_guid,Quantity,Period
                from main2
                where ra2=1
                    and Quantity <> 0
                """

        recomm_script_ProductFixesByTypeRecommendations_old2=f""" -- текущие рекомендации
                with maxdate as
                (
                SELECT  BranchID, ProductID, max(DateOfRecording) - Interval '4 Day' dt -- интервал - для долгого появления DeletionMark прошлого периода
                FROM {database_name}.{topic} main
                where 1=1
                    AND TypeRecommendationsID = '{recomm_type}'
                    and Period < (toDate(now()) + Interval '1 Day')
                group by BranchID, ProductID
                )
                ,
                main0 as
                (
                SELECT DateOfRecording, main.BranchID branch_guid,main.ProductID product_guid,Quantity,DeletionMark,Period,TypeRecommendationsID
                FROM {database_name}.{topic} main
                inner join maxdate on main.BranchID=maxdate.BranchID and main.ProductID=maxdate.ProductID
                where 1=1
                    AND TypeRecommendationsID = '{recomm_type}'
                    and Period < (toDate(now()) + Interval '1 Day')
                    and DateOfRecording >=maxdate.dt
                )
                ,
                main1 as
                (
                SELECT  branch_guid, product_guid,Quantity,DeletionMark,Period,TypeRecommendationsID
                FROM main0
                --where DeletionMark =0 -- под игнор пометки на удаление
                ORDER BY DateOfRecording DESC 
                LIMIT 1 BY branch_guid,product_guid,TypeRecommendationsID, Period
                )
                ,
                main2 as 
                (
                select branch_guid,product_guid,Quantity,Period
                from main1
                where 1=1
                    and DeletionMark =0
                ORDER BY Period DESC 
                LIMIT 1 BY branch_guid,product_guid,TypeRecommendationsID 
                )
                select branch_guid,product_guid,Quantity,Period
                from main2
                where 1=1
                    and Quantity <> 0
                """
                
        recomm_script_ProductFixesByTypeRecommendationsNew=f""" -- текущие рекомендации
                with main as
                (
                SELECT main.BranchID branch_guid,main.ProductID product_guid,argMax(Quantity,Period) Quantity
                FROM {database_name}.{topic} main
                where 1=1
                    AND TypeRecommendationsID = '{recomm_type}'
                group by BranchID, ProductID
                )
                select branch_guid,product_guid,Quantity--,Period
                from main
                where 1=1
                    and Quantity <> 0
                """
                
        recomm_script_SalesPotentialsByTypeOfRecommendation=f""" -- текущие рекомендации
                with main0 as
                (
                SELECT DateOfRecording, BranchID branch_guid,CategoryID category_guid,MinCount,MaxCount,Period,TypeRecommendationID, MeasureID
                FROM {database_name}.{topic} main
                left join dns_retail.BranchHier BranchHier on main.BranchID = BranchHier.BranchGuid 
                where 1=1
                    AND TypeRecommendationID = '{recomm_type}'
                    AND DivisionName = '{DivisionName}'
                    and Period < (toDate(now()) + Interval '1 Day')
                )
                ,
                main1 as
                (
                SELECT row_number(*) over(partition by branch_guid,category_guid,TypeRecommendationID, Period order by DateOfRecording desc) ra1,*
                FROM main0
                )
                ,
                main2 as 
                (
                select row_number(*) over(partition by branch_guid,category_guid,TypeRecommendationID order by Period desc) ra2,*
                from main1
                where 1=1
                    and ra1=1
                )
                select branch_guid,category_guid,MinCount,MaxCount,MeasureID
                from main2
                where ra2=1
                    and MinCount <> 0
                """
        
        # если требуется массовое зануление, то обнуляем таблицу с новыми рекомендациями
        if to_null==True:
            main=main[0:0]
        
        # загружаем в датафрейм актуальные рекомендации
        
        try:
            recomm_script=eval('recomm_script_'+topic)
        except:
            raise Exception('В new_recomm не задан скрипт выгрузки СрезПоследних для таблицы ' + topic)
        
        recomm=df_build(database_conn,recomm_script,check_null=check_null)
        
        
        scr="""
         select guid "TypeRecommendationsID"
            from etl.calculus c 
            where over_capacity =0
        """
        #not_over_capacity=df_build('delta_bi',scr).TypeRecommendationsID.to_list() 
        #if recomm_type in not_over_capacity:
        #    recomm=recomm[0:0] # для разовой полной перегрузки

            
            
        #print(eval('recomm_script_'+topic))
        #print(recomm_type)
        
        # логируем колво рекомендаций в 1С
        #to_sql(recomm,'kafka_test_1с')
        timemark2('log_new.txt',task_uuid,'Колво рекомендаций в 1С',str(recomm.shape[0]))
        if recomm.shape[0]==0 and main.shape[0]==0:
            toSkype('Рекомендаций нет - ни в 1С, ни в Питоне','Airflow')
            raise Exception('Рекомендаций нет - ни в 1С, ни в Питоне')
            
        if (recomm.shape[0]/max(main.shape[0],0.001))>20 and recomm.shape[0]>100 and to_null==False: # т.к. мелочь может и занулиться
            toSkype('Резкое сокращение колва рекомендаций','Airflow')
            raise Exception('Резкое сокращение колва рекомендаций ' + topic)
        #with open('log_new.txt', "a") as f:
        #    f.write('\n'+task_uuid+';')
        #    f.write('Колво рекомендаций в 1С;')
        #    f.write(str(recomm.shape[0])+';')
        
        #if topic=='ProductExposure_PrioritizationCoefficients':
        #if recomm_type=='d0f607a0-7433-11ec-8f5e-00155d8ed20c':
        #import datetime
        #to_csv(recomm,'recomm '+recomm_type +datetime.datetime.now().strftime("%Y-%m-%d %H-%M"))

        # загружаем в датафрейм категории
        #categories=df_build("bi_fois",'select category_guid,guid product_guid from dict_product where is_deleted =false')

        # объединяем актуальные рекомендации со свежевычисленными
        # такой fillna - чтобы при загрузке дробных значений изначально попадали данные меньше порога
        #toSkype('sending to sql','Airflow')
        #to_sql(main,'check_negative_main',n_jobs=1)
        #to_sql(recomm,'check_negative_recomm',n_jobs=1)
        #toSkype('sended to sql','Airflow')
        if main.shape[0]<7000000 and recomm.shape[0]<7000000:
            #toSkype('merge recomm','Airflow')
            main=main.merge(recomm,how='outer',on=merge_on)
        elif main.shape[0]<14000000 and recomm.shape[0]<14000000:
            toSkype('начали жуткий merge','Airflow')
            main=split_merge(main,recomm,how='outer',on=merge_on,col=merge_on[0],partion_value=8,reset=False)
            toSkype('закончили жуткий merge','Airflow')
        else:
            toSkype('начали жуткий merge','Airflow')
            main=split_merge(main,recomm,how='outer',on=merge_on,col=merge_on[0],partion_value=4,reset=False)
            toSkype('закончили жуткий merge','Airflow')
        
        
        #main[value_col['new_count']]=main[value_col['new_count']].fillna(-threshold)#-threshold+0.0000000000001
        #main['new_count']=main['new_count'].fillna(-threshold)#-threshold+0.0000000000001
        
        #main.new_count=main.new_count.astype('float')
        #main[value_col['new_count']]=main[value_col['new_count']].astype('float')
        
        #main['diff']=abs(main.new_count-main[value_col['new_count']])
        
        #toSkype('apply threshold','Airflow')
        for col1 in value_col.keys():
            main[col1]=main[col1].fillna(-threshold)
            main[col1]=main[col1].astype('float')
        for col2 in value_col.values():
            main[col2]=main[col2].fillna(-threshold)
            main[col2]=main[col2].astype('float')
        
        #toSkype('create diff','Airflow')
        print('create diff')
        main['diff']=0
        #toSkype('create diff1','Airflow')
        print('create diff1')
        for col1,col2 in value_col.items():
            main['diff']=main['diff']+abs(main[col1]-main[col2])
        #toSkype('create diff2','Airflow')
        print('create diff2')
        main=main.query(f'diff>{threshold}')
        
        #toSkype('reset_index','Airflow')
        main=main.reset_index(drop=True)
        if DivisionName!='':
            toSkype('сформировали diff','Airflow')
        # собираем статистику по рекомендациям - новая, изменённая или зануление
        # с модулем - т.к. зануление может произойти двумя способами
        # либо мы в main вычислим ноль, либо в main вообще пропадёт связка филиал-продукт (тогда встанет порог)
        # последние пункты выбора - как раз мелкие новые, которые изначально меньше порога, но надо загружать
        #conditions = [ 
        #    (main.new_count.between(-threshold,0)==False) & (main[value_col['new_count']]!=-threshold),
        #    (main.new_count.between(-threshold,0))        & (main[value_col['new_count']]!=-threshold),
        #    (main.new_count.between(-threshold,0)==False) & (main[value_col['new_count']]==-threshold)]
            
        # пробуем так. посмотрим. Посмотрел - нормас
        for col1 in value_col.keys():
            main.loc[main[col1]==-threshold,col1]=0
        for col2 in value_col.values():
            main.loc[main[col2]==-threshold,col2]=0
        if DivisionName!='':
            toSkype('вернули нули','Airflow')
        
        #toSkype('create new old count','Airflow')
        if len(value_col)>1:
            main['_new_count']=0
            main['_old_count']=0
                
            for col1 in value_col.keys():
                main['_new_count']=main['_new_count']+abs(main[col1])
            for col2 in value_col.values():
                main['_old_count']=main['_old_count']+abs(main[col2])
                
            #main['diff2']=abs(main['_old_count']-main['_new_count'])
            conditions0 = [ 
                (main['_new_count']!=0) & (main['_old_count']!=0),
                (main['_new_count']==0) & (main['_old_count']!=0),
                (main['_new_count']!=0) & (main['_old_count']==0)]
        else:
            new_count=list(value_col.keys())[0]
            old_count=list(value_col.values())[0]
            
            conditions0 = [ 
                (main[new_count]!=0) & (main[old_count]!=0),
                (main[new_count]==0) & (main[old_count]!=0),
                (main[new_count]!=0) & (main[old_count]==0)]
            
            #(main.new_count.between(threshold,0)) & (main.Quantity==-threshold)]
        #toSkype('choices','Airflow')
        choices = ['изменение рекомендаций','зануление старых','новые']
        main['status']=''
        if DivisionName!='':
            toSkype('перед статусом '+str(main.shape[0]),'Airflow')
        if main.shape[0]<9000000:
            main['status'] = np.select(conditions0, choices, default=False)
        else:
            for i in range(len(conditions0)):
                partion_value=4
                brans=main.branch_guid.drop_duplicates().to_list()
                brans=[ brans[x:x+partion_value] for x in range(0,len(brans),partion_value)]
                #for branch_guid in main.branch_guid.drop_duplicates():
                #    main.loc[conditions0[i] & (main.branch_guid==branch_guid),'status']=choices[i]
                for br_list in brans:
                    main.loc[conditions0[i] & (main.branch_guid.isin(br_list)),'status']=choices[i]
        #toSkype('end choices','Airflow')
        if DivisionName!='':
            toSkype('сформировали status','Airflow')
        # порог использовали для статистики и выборки, а для заливки меняем на честный ноль
        #main.loc[main.new_count==-threshold,'new_count']=0
        
        if topic=='ProductExposure_PrioritizationCoefficients':
            #import datetime
            #to_csv(main,'itog full '+recomm_type +' '+datetime.datetime.now().strftime("%Y-%m-%d %H-%M"))
            #x=input()
            pass
        
        # берём только отличные от актуальных
        #itog=main.query('new_count!=Quantity')
        
        
        #if DivisionName!='':
        #    toSkype('отсекли трешхолд','Airflow')
        #itog.loc[:,'TypeRecommendationsID']=recomm_type
        main['TypeRecommendationsID']=recomm_type
        if DivisionName!='':
            toSkype('вставили рекомм_типе','Airflow')
        # подключаем категории
        #itog=itog.merge(categories,how='left')
        
        
        scr="""
         select guid "TypeRecommendationsID"
            from etl.calculus c 
            where over_capacity =0
        """
        not_over_capacity=df_build('delta_bi',scr).TypeRecommendationsID.to_list()
        if recomm_type in not_over_capacity:
            main['OverCapacity']=False
        else:
            main['OverCapacity']=True
        
        return main
    except Exception as e:
        #+recomm_script
        df_load('delta_bi','etl.log_errors',pd.DataFrame({'task_guid':task_uuid,'error_message':str(traceback.format_exc())[:1000].replace("'",""),'dt':datetime.datetime.now(tz)}, index=[0]),columns='')
        print(str(traceback.format_exc()))
        raise Exception(str(e))

def load_hist(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку). 
        recomm_type(str) - guid рекомендации
    ):
    Выход:
        (dict) - результат работы. 
    """
    
    # автор = система
    kafka_dict = {
        'Автор': '7ea772db-8713-4eb2-8f5d-4287ba5d35ae'  
        ,'ВидРекомендацииРаспределения': row['TypeRecommendationsID']#recomm_type
        ,'ДатаЗаписи': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')  
        ,'Категория': str(row['category_guid']) 
        ,'КлючПартииЗаписи': guid_key  
        ,'Количество': int(row['new_count'])
        ,'Период': datetime.datetime.now().strftime('%Y-%m-%dT00:00:00') 
        ,'Товар': str(row['product_guid']) 
        ,'Филиал': str(row['branch_guid'])  
    }

    key = str(row['branch_guid']) + str(row['product_guid']) + row['TypeRecommendationsID']
    return {"key": key, "value": kafka_dict}

def load_SalesForecast(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку). 
        recomm_type(str) - guid рекомендации
    ):
    Выход:
        (dict) - результат работы. 
    """
    # автор = система Quantity
    
    kafka_dict = {
        #'TYPE_Основание': 'СправочникСсылка.АвтораспределениеВидыРекомендаций'
        'Автор': '7ea772db-8713-4eb2-8f5d-4287ba5d35ae'
        ,'ВидРекомендации': row['TypeRecommendationsID']#recomm_type
        ,'КлючПартииЗаписи': guid_key
        ,'Количество': row['new_count']
        #,'Основание': row['TypeRecommendationsID']#recomm_type
        ,'Период': datetime.datetime.now().strftime('%Y-%m-%dT00:00:00') if row['new_count']!=0 else row['Period'].strftime('%Y-%m-%dT00:00:00')
        ,'ПометкаУдаления': False if row['new_count']!=0 else True
        #,'СверхЕмкости': True        
        ,'Товар': row['product_guid']
        ,'Филиал': row['branch_guid']  
    }
    key = row['TypeRecommendationsID'] + str(row['branch_guid']) + str(row['product_guid']) 
    return {"key": key, "value": kafka_dict}

def load_fixed(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку). 
        recomm_type(str) - guid рекомендации
    ):
    Выход:
        (dict) - результат работы. 
    """
    # автор = система
    
    kafka_dict = {
        'TYPE_Основание': 'СправочникСсылка.АвтораспределениеВидыРекомендаций'
        ,'Автор': '7ea772db-8713-4eb2-8f5d-4287ba5d35ae'
        ,'ВидРекомендации': row['TypeRecommendationsID']#recomm_type
        ,'КлючПартииЗаписи': guid_key
        ,'Количество': int(row['new_count']) #if row['new_count']!=0 else int(row['Quantity'])
        ,'Основание': row['TypeRecommendationsID']#recomm_type
        ,'Период': datetime.datetime.now().strftime('%Y-%m-%dT00:00:00') #if row['new_count']!=0 else row['Period'].strftime('%Y-%m-%dT00:00:00')
        ,'ПометкаУдаления': False if row['new_count']!=0 else True
        ,'СверхЕмкости': row['OverCapacity']        
        ,'Товар': row['product_guid']
        ,'Филиал': row['branch_guid']  
        ,'TYPE_Товар': 'СправочникСсылка.Номенклатура'
    }
    key = str(row['branch_guid']) + str(row['product_guid']) + row['TypeRecommendationsID']
    return {"key": key, "value": kafka_dict}

def load_offline_coeff_old(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку)
    ):
    Выход:
        (dict) - результат работы. 
    """
    # автор = система
    
    kafka_dict = {
        'КлючПартииЗаписи': guid_key
        ,'Период': datetime.datetime.now().strftime('%Y-%m-%dT00:00:00')
        ,'Филиал': row['branch_guid']  
        ,'Товар': row['product_guid']
        ,'Количество': row['new_count']
        ,'Автор': '7ea772db-8713-4eb2-8f5d-4287ba5d35ae'
    }
    key = str(row['branch_guid']) + str(row['product_guid'])
    return {"key": key, "value": kafka_dict}

def load_offline_coeff(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку)
    ):
    Выход:
        (dict) - результат работы. 
    """
    # автор = система
    
    kafka_dict = {
        'КлючПартииЗаписи': guid_key
        ,'Период': datetime.datetime.now(tz).strftime('%Y-%m-%dT00:00:00')
        ,'Филиал': row['branch_guid']  
        ,'Товар': row['product_guid']
        ,'МестоХранения': '00000000-0000-0000-0000-000000000000'
        ,'Количество': row['new_count']
        ,'Офлайн': False
        ,'Автор': '7ea772db-8713-4eb2-8f5d-4287ba5d35ae'
        ,'ПометкаУдаления': False
    }
    key = str(row['branch_guid']) + str(row['product_guid'])
    return {"key": key, "value": kafka_dict}

def load_sales_potential(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку)
    ):
    Выход:
        (dict) - результат работы. 
    """
    # автор = система
    dt=datetime.datetime.now()
    kafka_dict = {
          "Период": dt.strftime('%Y-%m-%dT00:00:00')
          ,"Филиал": row['branch_guid']  
          ,"Категория": row['category_guid']  
          ,"ВидРекомендации": row['TypeRecommendationsID']#"d0f607a0-7433-11ec-8f5e-00155d8ed20c"
          ,"ЕдиницаИзмерения": row['MeasureID']
          ,"МинКоличество": round(row['min_new_count']-0.00000001) # т.к. 1.5 должно округляться вниз, заказ Саши Борсина
          ,"ОптКоличество": round(row['opt_new_count']-0.00000001)   # т.к. 1.5 должно округляться вниз, заказ Саши Борсина
          ,"МаксКоличество": round(row['max_new_count']-0.00000001)   # т.к. 1.5 должно округляться вниз, заказ Саши Борсина
          ,"Автор": "7ea772db-8713-4eb2-8f5d-4287ba5d35ae"
          ,"КлючПартииЗаписи": guid_key
    }
    #key = str(row['branch_guid']) + str(row['category_guid'])
    #print(kafka_dict)str(dt.strftime('%Y-%m-%d'))
    key=str(dt.strftime('%Y-%m-%d'))+kafka_dict["Филиал"]+kafka_dict["Категория"]+kafka_dict["ВидРекомендации"]+kafka_dict["ЕдиницаИзмерения"]
    return {"key": key, "value": kafka_dict}
# '10.0.75.197:9092'

def load_potential_PP(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку)
    ):
    Выход:
        (dict) - результат работы. 
    """
    # автор = система
    
    kafka_dict = {
        'КлючПартииЗаписи': guid_key
        ,'ВидРасчета': row['TypeRecommendationsID']
        ,'Филиал': row['branch_guid']  
        ,'Товар': row['product_guid']
        ,'Количество': round(row['new_count'],3)
    }
    key = str(kafka_dict['Филиал']) + str(kafka_dict['Товар'])+ str(kafka_dict['ВидРасчета'])
    return {"key": key, "value": kafka_dict}

def load_AdditionalPrioritizationCoefficients(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку)
    ):
    Выход:
        (dict) - результат работы. 
    """
    # автор = система
    
    kafka_dict = {
        'КлючПартииЗаписи': guid_key
        ,'Филиал': row['branch_guid']  
        ,'Товар': row['product_guid']
        ,'Коэффициент': round(row['new_count'],8)
    }
    key = ''#str(kafka_dict['Филиал']) + str(kafka_dict['Товар'])+ str(kafka_dict['ВидРасчета'])
    return {"key": key, "value": kafka_dict}

def load_forecast_weekly(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку)
    ):
    Выход:
        (dict) - результат работы. 
    """
    

    kafka_dict = {
          "НеделяПлана": row['ds'].strftime('%Y-%m-%dT00:00:00')
          ,"Категория": row['category_guid']  
          ,"Количество": row['count_fc']  
          ,"Оборот": row['income_fc']
          ,"Себестоимость": row['cost_fc']
          ,"ВидПрогноза": row['model']  
    }
    

    key=str(row['ds'].strftime('%Y-%m-%dT00:00:00'))+kafka_dict["Категория"]

    return {"key": key, "value": kafka_dict}

def load_forecast_rrc_weekly(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку)
    ):
    Выход:
        (dict) - результат работы. 
    """
    

    kafka_dict = {
          "НеделяПлана": row['ds'].strftime('%Y-%m-%dT00:00:00')
          ,"Склад": row['rrc_guid']  
          ,"Категория": row['category_guid']  
          ,"ВидПрогноза": row['model']  
          ,"Количество": row['count_fc']  
          ,"Оборот": row['income_fc']
          ,"Себестоимость": row['cost_fc']
          ,"ДатаЗаписи": row['dt'].strftime('%Y-%m-%dT00:00:00')
    }
    

    key=str(row['ds'].strftime('%Y-%m-%dT00:00:00'))+kafka_dict["Категория"]+kafka_dict['Склад']

    return {"key": key, "value": kafka_dict}

def load_forecast_monthly(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку)
    ):
    Выход:
        (dict) - результат работы. 
    """

    kafka_dict = {
          "Категория": row['category_guid']  
          ,"МесяцПлана": row['ds_month'].strftime('%Y-%m-%dT00:00:00')
          ,"Количество": row['count_fc']  
          ,"Оборот": row['income_fc']
          ,"Себестоимость": row['cost_fc']
          ,"ВидПрогноза": row['model']  
    }
    
    key=kafka_dict["Категория"]+str(row['ds_month'].strftime('%Y-%m-%dT00:00:00'))

    return {"key": key, "value": kafka_dict}
    
    
def load_forecast_decomposed_final(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку)
    ):
    Выход:
        (dict) - результат работы. 
    """

    kafka_dict = {
          "Товар": row['ProductID'] 
          ,"НеделяПлана": row['PlanWeek'].strftime('%Y-%m-%dT00:00:00')
          ,"Склад": row['BranchStoreHouseID']
          ,"Количество": float(row['Count'])
          ,"Оборот": float(row['SalesSum'])
          ,"Себестоимость": float(row['CostSum'])
          ,"ВидПлана": row['PlanType']
          ,"ДатаЗаписи": row['DateRecording']#.strftime('%Y-%m-%dT%H:%M:%S')   
          ,"Автор": "7ea772db-8713-4eb2-8f5d-4287ba5d35ae"
    }
    
    key=row["ProductID"]+str(row['PlanWeek'].strftime('%Y-%m-%dT00:00:00'))+row['BranchStoreHouseID'] 

    return {"key": key, "value": kafka_dict}


def load_turnover(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку)
    ):
    Выход:
        (dict) - результат работы. 
    """
#"Филиал": row['branch_guid'] ,
          
    kafka_dict = {
          "Филиал": '00000000-0000-0000-0000-000000000000'
          ,"Категория": row['CategoryID']
          ,"МинимальнаяОборачиваемость": int(row['min_turnover'])
          ,"ОптимальнаяОборачиваемость": int(row['optimum_turnover'])
          ,"МаксимальнаяОборачиваемость": int(row['max_turnover'])
          ,"Автор": "7ea772db-8713-4eb2-8f5d-4287ba5d35ae"
    }
    
    key=row['CategoryID']

    return {"key": key, "value": kafka_dict}
  
  
def load_CreationOfDocumentsForDistribution(row,guid_key): 
    """
    Сериализация json для последующей передачи в kafka. 
    Вход: 
        row(object) - строка таблицы с данными. 
        guid_key(str) - guid записи в kafka (1 на всю пачку)
    ):
    Выход:
        (dict) - результат работы. 
    """

    kafka_dict = {
          "Данные": row['data'] 
    }
    
    key=''

    return {"key": key, "value": kafka_dict}
    
# '10.0.75.197:9092'
#
def push_kafka(itog,recomm_type,servers = ['10.10.1.171:9092', '10.10.1.172:9092', '10.10.1.173:9092'],to_null=False,comm='',task_uuid='',topic=['ProductFixesByTypeRecommendationsNew','ProductFixesByTypeRecommendations'],max_messages=700000,database='dns_log',country='ru',click_check=True,by_suffix=False):
    """
    Передача в kafka. 
    Вход: 
        itog(DataFrame) - таблица с данными
        recomm_type(str) - guid рекомендации
        servers - брокеры kafka
        to_null (bool) - если необходимо массовое зануление ранее загруженных рекомендаций
        comm (str) - комментарий в лог (при необходимости).
        task_uuid (uuid) - гуид задачи (для логирования)
        topic (str) - название топика в Кафке
    """
    try:
        if topic=='ProductFixesByTypeRecommendations':
            topic=['ProductFixesByTypeRecommendationsNew','ProductFixesByTypeRecommendations']
            
        if isinstance(topic,list):
            topic_check=topic[0]
            topic_upload=topic[1]
        elif isinstance(topic,str):
            topic_check=topic
            topic_upload=topic    
            
            
        if by_suffix:
            if 'topic_suffix' in itog.columns:
                itog=itog.drop(columns='topic_suffix')
            
            ibd=df_build('dns_dwh','branches_dnsdwh_ibd')
            itog=itog.merge(ibd,left_on='branch_guid',right_on='branch_guid2')
            
            for topic_suffix in itog[~itog.topic_suffix.isna()].topic_suffix.unique():
                itog_temp=itog.query(f'topic_suffix=="{topic_suffix}"')
                push_kafka(itog_temp,recomm_type,topic=[topic_upload,topic_upload+topic_suffix],click_check=False)
                print(topic_suffix)
            
            return ''
        #print('начали')
        # логируем начало Кафки
        #timemark('log.txt')
        timemark2('log_new.txt',task_uuid,'Загрузка')
        #if itog.isna().sum().sum()>0:
        #    raise ValueError('В Кафку пытаются отправить NaN, '+topic)
        #elif (itog==np.inf).sum().sum()>0:
        #    raise ValueError('В Кафку пытаются отправить inf, '+topic)
        # иногда топик проверки не совпадает с топиком загрузки (при загрузке в региональные ИБД)
        # поэтому можем передать два топика в списке
        
            
        if isinstance(database,list):
            database_conn=database[0]+(('_'+country) if country!='ru' else '')
            database_name=database[1]
        elif isinstance(database,str):
            database_conn=database+(('_'+country) if country!='ru' else '')
            database_name=database
        
        if country=='kz':
            servers=['kz-kafka1.dns-shop.kz:9092','kz-kafka2.dns-shop.kz:9092','kz-kafka3.dns-shop.kz:9092']
        
        # определяем функцию джисонификации
        # делаем так, чтобы хранить словарь централизованно
        try:
            json_function=eval(json_functions[topic_check])
        except:
            raise Exception('Нужно в my_functions.json_functions внести сопоставление топика и его функции подачи данных')
        
        
        if itog.empty:
            return 'bad','Новых данных нет'

        guid_key = str(uuid.uuid4())
        
        
        
        if click_check:
        
            # делаем проверку, чтобы при сбоях на предыдущих этапах избежать глобального зануления рекомендаций
            # логируем ошибку
            try: # т.к. иногда расширяемся, и грузим не только new_count
                if abs(itog._new_count.sum())<2 and itog._new_count.count()>2000 and to_null==False:
                    return 'bad','Подозрение на массовое зануление'
            except:
                if abs(itog.new_count.sum())<2 and itog.new_count.count()>2000 and to_null==False:
                    return 'bad','Подозрение на массовое зануление'
                    
                    
            # ищем в регистре максимальную дату по рекомендации.
            # Далее используем для определения количества загрузившихся строк
            to_check=f"""
            SELECT max(main.Period) maxdate
            FROM {database_name}.{topic_check} main
                --left join dns_retail.BranchHier BranchHier on main.BranchID = BranchHier.BranchGuid
            where {recomm_column[topic_check]} ='{recomm_type}'
            """
            # из-за казахского клика, где нет справочника филиалов
            # сначала пробуем без справочника, потом с ним
            if country=='ru':
                maxdate=df_build(database_conn,to_check.replace('--',''),show_time=False).maxdate[0]
            else:
                #to_check=to_check.replace('--','')
                maxdate=df_build(database_conn,to_check,show_time=False).maxdate[0]
            
        # Создаем объект для передачи в Kafka
        #max_messages = itog.shape[0]
        bootstrap_servers = ','.join(servers)
        prd_conf = {
            'bootstrap.servers': bootstrap_servers
            ,'queue.buffering.max.messages': max_messages
        }
        producer = Producer(prd_conf)
        
        #headers = [("_operation_", "del")]
        
        cnt_pushed=0
        # Передаем записи
        for index, row in itog.iterrows():
            res_recommendation=json_function(row, guid_key)
            #print(str(res_recommendation))
            try:
                producer.poll(0)
            except BufferError:
                print('Ожидаем разбора очереди, poll')
                sleep(30) # если очередь переполнилась - подождём, пока рассосётся
                producer.poll(0)
            except Exception as e: # в случае иной ошибки - прекращаем работу, пишем ошибку в лог
                print('Kafka poll \n'+str(traceback.format_exc()))
                comm+=(", ", "")[len(comm)==0]+ 'Error poll - '+str(e.__class__.__name__)+';\n '+str(sys.exc_info()[1])
                return 'bad',comm
            #print(res_recommendation['value'])
            
            
            if 'ПометкаУдаления' in res_recommendation['value'].keys():
                if res_recommendation['value']['ПометкаУдаления']==True:
                    producer.produce(
                        topic = topic_upload
                        ,value = json.dumps(res_recommendation['value'], ensure_ascii = False)
                        ,key = res_recommendation['key']
                        ,headers = [("_operation_", "del")]
                    )
                    print('hz')
                else:
                    producer.produce(
                        topic = topic_upload
                        ,value = json.dumps(res_recommendation['value'], ensure_ascii = False)
                        ,key = res_recommendation['key']
                    )
            else:
                producer.produce(
                        topic = topic_upload
                        ,value = json.dumps(res_recommendation['value'], ensure_ascii = False)
                        ,key = res_recommendation['key']
                    )
            cnt_pushed+=1
            # принудительная пауза при больших загрузках. Почему-то try не всегда отлавливает BufferError
            # А иногда Кафка сама отваливается, вываливая монструозную ошибку
            if (cnt_pushed+1) % 50000==0:
                
                
                # если места в очереди осталось ровно на следующую партию
                # каждое сообщение уходит в три брокера, поэтому умножаем на три
                lenp=len(producer)
                if lenp>(max_messages-3*50000): 
                    print(lenp,max_messages-3*50000,'Перерыв для Кафки -',str(cnt_pushed+1))
                    sleep(10)
                    
        try:
            producer.flush()
        except BufferError:
            print('Ожидаем разбора очереди, flush')
            sleep(30) # если очередь переполнилась - подождём, пока рассосётся
            producer.flush()
        except Exception as e: # в случае иной ошибки - прекращаем работу, пишем ошибку в лог
            print('Kafka flush \n'+str(traceback.format_exc()))
            comm+=(", ", "")[len(comm)==0]+ 'Error flush - '+str(e.__class__.__name__)+';\n '+str(sys.exc_info()[1])
            return 'bad',comm
        print('Готово')
        # Логируем конец Кафки
        timemark2('log_new.txt',task_uuid,'Конец')
        
        
        
        if click_check:
            # ставим задержку, чтобы Кафка всё провела
            sleep(30)
            
            # выясняем, сколько рекомендаций прогрузилось в регистр
            
            to_check=F"""
            SELECT count(*) cnt
            FROM {database_name}.{topic_check} main
                --left join dns_retail.BranchHier BranchHier on main.BranchID = BranchHier.BranchGuid
            where {recomm_column[topic_check]} ='{recomm_type}'
                and main.Period>'{maxdate}'
            """
            
            # из-за казахского клика, где нет справочника филиалов
            # сначала пробуем без справочника, потом с ним
            if country=='ru':
                pushed=df_build(database_conn,to_check.replace('--',''),show_time=False)
            else:
                #to_check=to_check.replace('--','')
                pushed=df_build(database_conn,to_check,show_time=False)
            #toSkype(str(pushed.cnt[0])+'\n'+to_check,'Airflow')
            # если провело не все строки, возможно Кафка перегружена. И за прошедшие 30 секунд данные не прогрузились
            # подождём минуту и проверим снова
            if pushed.cnt[0]<cnt_pushed:
                print('Провело мало строк. Ждём и повторяем проверку')
                sleep(120)
                if country=='ru':
                    pushed=df_build(database_conn,to_check.replace('--',''),show_time=False)
                else:
                    #to_check=to_check.replace('--','')
                    pushed=df_build(database_conn,to_check,show_time=False)
            
            if pushed.cnt[0]==0:
                #return 'bad','Загружено ноль строк'
                to_sql(itog,'check_kafka_'+datetime.datetime.now(tz).strftime("%Y_%m_%d_%H_%M"))
                pass # вдруг проведёт с задержкой, и жёстко задвоится
            elif pushed.cnt[0]<cnt_pushed:
                to_sql(itog,'check_kafka_'+datetime.datetime.now(tz).strftime("%Y_%m_%d_%H_%M"))
                pass
            
            # формируем статистику по видам рекомендаций
            stats=itog.status.value_counts()
            s=''
            for ind in stats.index:
                s+=ind+'-'+str(stats[ind])+', '
                
            # для красоты убираем знаки в конце
            s=s[:-2]
            # временно вырежу. Всё равно не использую
            #s+=';'+comm
            
            # логируем колво поданных на загрузку рекомендаций,
            # долю успешно загруженных, статистику в разрезе видов
            if task_uuid!='':
                try:
                    timemark2('log_new.txt',task_uuid,'Отправлено',str(cnt_pushed)) 
                    timemark2('log_new.txt',task_uuid,'Доля успешной загрузки',str(100*pushed.cnt[0]/cnt_pushed))  
                    timemark2('log_new.txt',task_uuid,'Статистика',s)  
                except Exception as e:
                    print("Ошибка при логировании")
            
            return 'ok',str(100*pushed.cnt[0]/cnt_pushed)+'; '+s+(' - '+recomm_type if topic=='ProductExposure_PrioritizationCoefficients' else '')
    except Exception as e:
        print(str(traceback.format_exc()))
        df_load('delta_bi','etl.log_errors',pd.DataFrame({'task_guid':task_uuid,'error_message':str(traceback.format_exc())[:1000]+database_conn,'dt':datetime.datetime.now(tz)}, index=[0]),columns='')
        return 'bad',str(sys.exc_info()[1])