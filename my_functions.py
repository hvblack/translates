import os
os.chdir(os.path.dirname(__file__)) 

import pandas as pd
import datetime
from time import sleep
# для распараллеливания
from joblib import Parallel, delayed
import traceback

import sys

import clickhouse_driver as cd

import requests

import pytz
tz = pytz.timezone('Asia/Irkutsk')


import psycopg2
from sqlalchemy import create_engine

dsn_database='dep_spb'
try:
    from connections import conn
    dsn_hostname = conn[dsn_database]['dsn_hostname']
    dsn_port = conn[dsn_database]['dsn_port']
    dsn_uid = conn[dsn_database]['dsn_uid']
    dsn_pwd = conn[dsn_database]['dsn_pwd']
    db_type = conn[dsn_database]['db_type']
    try:
        dsn_database = conn[dsn_database]['dsn_database']
    except:
        pass
    try:
        extra=conn[dsn_database]['extra']
    except:
        pass
except ModuleNotFoundError:
    from airflow.hooks.base import BaseHook
    connection = BaseHook.get_connection(dsn_database)
    dsn_hostname = connection.host
    dsn_port = connection.port
    dsn_uid = connection.login
    dsn_pwd = connection.password
    db_type = connection.description
    try:
        temp=connection.schema
        if str(temp)!='': # т.к. обычно совпадает с именем соединения, поэтому не прописываю
            dsn_database = connection.schema
    #print('Нет такого модуля')
    except:
        pass
    try:
        extra=connection.extra
    except:
        pass
dep_spb = create_engine(f'postgresql+psycopg2://{dsn_uid}:{dsn_pwd}@{dsn_hostname}/{dsn_database}')



def readfile(filename):
    with open(filename,encoding='utf-8-sig',mode='r') as f:# ,encoding='utf-8'
        s=f.read()
    return s
    
import uuid    
    
def toSkype(msg,chat='Лог'):
    """
    from skpy import Skype
    loggedInUser = Skype(login, password)

    for id in loggedInUser.chats.recent():
        x=str(loggedInUser.chats[id])
        if 'Логирование' in x: # Логирование - название вручную созданного чата
            print(x)
    
    from skpy import Skype
    
    try:
        from connections import conn
        dsn_uid = conn['skype']['dsn_uid']
        dsn_pwd = conn['skype']['dsn_pwd']
    except ModuleNotFoundError:
        from airflow.models import Variable
        dsn_uid = Variable.get("skype_login")
        dsn_pwd = Variable.get("skype_password")

    except:
        pass
    
    
    if chat=='Лог':
        chat_id='19:8f040a9b01cb4e0b8c4214bbe79fe6ef@thread.skype'
    elif chat=='Увед':
        chat_id='19:37c3b4edb09941b8adfc4efb2c7947be@thread.skype'
    elif chat=='Airflow':
        chat_id='19:4e4aa771f03347dfb502e4bd6946ebe2@thread.skype'
    elif chat=='Прогнозы':
        chat_id='19:bb0dbc12ef8444f79e146c532e358149@thread.skype'
        
    cnt=0
    while cnt<20:
        cnt+=1
        try:
            loggedInUser = Skype(dsn_uid, dsn_pwd)
            ch=loggedInUser.chats[chat_id]
            ch.sendMsg(msg)
        except Exception as e:
            print(e.__class__.__name__)
            sleep(5)
        else:
            break
            
    if cnt==20:
        print('Скайп не принял')
    """
    #https://api.telegram.org/bot<YourBOTToken>/getUpdates - выяснение ИД
    
    
    try:
        from airflow.hooks.base import BaseHook
        connection = BaseHook.get_connection('telegram')
        TOKEN = connection.password
    except ModuleNotFoundError:
        from connections import conn
        TOKEN = conn['telegram']['token']
    
    #TOKEN = '5766025582:AAEbonK0FNA-RxClWt4FjNYlILq0ln9nII8'
    
    if chat=='Лог':
       #CHAT_ID = '862661567'
        CHAT_ID='-895452928'
    elif chat=='Увед':
        CHAT_ID='-632279886'
    elif chat in ('Airflow','A'):
        CHAT_ID='-845515563'
    elif chat=='Прогнозы':
        CHAT_ID='-882229152'
    
    SEND_URL = f'https://api.telegram.org/bot{TOKEN}/sendMessage'
    
    cnt=0
    while cnt<20:
        cnt+=1
        try:
            requests.post(SEND_URL, json={'chat_id': CHAT_ID, 'text': msg}) 
        except Exception as e:
            print(e.__class__.__name__)
            sleep(5)
        else:
            break

    
def df_load_main(dsn_database,tablename,df,columns='',try_cnt=10):

    if df.empty:
        return ''
    df=df.fillna('isna')
    # чтобы общий скрипт на свой комп и airflow
    dsn_database_orig=dsn_database+''
    try:
        from connections import conn
        dsn_hostname = conn[dsn_database]['dsn_hostname']
        dsn_port = conn[dsn_database]['dsn_port']
        dsn_uid = conn[dsn_database]['dsn_uid']
        dsn_pwd = conn[dsn_database]['dsn_pwd']
        dsn_database_orig=dsn_database
        db_type = conn[dsn_database]['db_type']
        try:
            dsn_database = conn[dsn_database]['dsn_database']
        except:
            pass
    except ModuleNotFoundError:
        from airflow.hooks.base import BaseHook
        connection = BaseHook.get_connection(dsn_database)
        dsn_hostname = connection.host
        dsn_port = connection.port
        dsn_uid = connection.login
        dsn_pwd = connection.password
        db_type = connection.description
        try:
            temp=connection.schema
            if str(temp)!='': # т.к. обычно совпадает с именем соединения, поэтому не прописываю
                dsn_database = connection.schema
        except:
            pass
        #print('Нет такого модуля')
    except:
        pass
    
    try:
        df=df.reset_index()
    except:
        pass

    #print(datetime.datetime.now())         
    if db_type=='postgre':#dsn_database in ['delta_bi','mart_fls','bi_fls','dep_spb']:
        import psycopg2
        
        cnt=0
        while cnt<=try_cnt:
            cnt+=1
            try:
                conn = psycopg2.connect(dbname=dsn_database, user=dsn_uid, 
                                password=dsn_pwd, host=dsn_hostname) 
            except Exception as e:
                print(e)
                sleep(15)
                if cnt==try_cnt:
                    raise Exception(str(e))
            else:
                break      
                
        cursor = conn.cursor()

        s_pattern=f"""
            INSERT INTO {tablename}
            ("{'","'.join(columns.keys())}") VALUES (
            """
        s=s_pattern
        #print(datetime.datetime.now()) 
        for b,row in df[columns.values()].iterrows():
            for i in row[:]:
                if "('" in str(i):
                    s+=str(i)+","
                    #print(i)
                elif i=='isna':
                    s+='NULL,'
                else:
                    s+="'"+str(i).replace("'","")+"',"
            s=s[:-1]
            s+=')'
            if (b+1) % 5000==0 or (b+1)==df.shape[0]:
                #print(s)
                #print(datetime.datetime.now())
                
                cnt=0
                while cnt<=try_cnt:
                    cnt+=1
                    try:
                        #print(s)
                        #print(datetime.datetime.now(),'перед execute')
                        cursor.execute(s)
                        conn.commit()
                        #print(datetime.datetime.now(),'после execute')
                    except Exception as e:
                        
                        print(e)
                        sleep(15)
                        if cnt==try_cnt:
                            raise Exception(str(e))
                    else:
                        break

                #conn.commit()

                s=s_pattern
            else:
                s+=',\n('
                
        conn.commit()
        cursor.close()
        
        conn.close()
    
    elif db_type=='clickhouse':#dsn_database in ['dns_log',] or dsn_database_orig in ['dsn_database_test','adm_fcs4','vlad']:
        import clickhouse_driver as cd

        if dsn_database_orig=='vlad':
            client = cd.Client(database=dsn_database,  host=dsn_hostname,
                                settings=dict(numpy_columns=True),sync_request_timeout=5)
        else:
            client = cd.Client(database=dsn_database, user=dsn_uid, 
                                    password=dsn_pwd, host=dsn_hostname,
                                    settings=dict(numpy_columns=True),sync_request_timeout=5) 
                                
        s_pattern=f"""
            INSERT INTO {tablename}
            ("{'","'.join(columns.keys())}") VALUES (
            """
        s=s_pattern
        for b,row in df[columns.values()].iterrows():
            for i in row[:]:
                if "('" in str(i):
                    #print('xxx')
                    s+=str(i)+","
                else:
                    s+="'"+str(i).replace("'","")+"',"
            s=s[:-1]
            s+=')'
            if (b+1) % 5000==0 or (b+1)==df.shape[0]:#5000
                #print(s)
                #print(datetime.datetime.now())
                cnt=0
                while cnt<=try_cnt:
                    cnt+=1
                    try:
                        client.execute(s)
                    except Exception as e:
                        #print(s)
                        print(e)
                        if cnt==try_cnt:
                            raise Exception(str(e))
                        sleep(3)
                    else:
                        break
                
                s=s_pattern
            else:
                s+=',('                        
        client.disconnect()


def df_load(dsn_database,tablename,df,columns='',n_jobs=1,try_cnt=1,check_columns=True,show_time=True):
    # если подали словарь - преобразуем в датафрейм
    
    if isinstance(df,dict):
        df=pd.DataFrame(df, index=[0])
    if columns=='':
        columns=dict()
        for col in df.columns:
            columns[col]=col
    
    #if df.shape[0]<=30000:
    #    n_jobs=1
    
    if check_columns:
        try:
            test_columns=df_build(dsn_database,f'select top 1 * from {tablename}',show_time=False)
        except:
            test_columns=df_build(dsn_database,f'select * from {tablename} limit 1',show_time=False)
        
        for col in columns.keys():
            if col not in test_columns.columns:
                df=df.drop(columns=columns[col])
                
        # на случай, если удалили столбцы
        columns=dict()
        for col in df.columns:
            columns[col]=col
            
    #n_jobs=14
    to_thread_optimum=400000 # чтобы избежать слишком больших кусков
    to_thread=df.shape[0]//n_jobs+1
    to_thread=min(to_thread_optimum,to_thread)
    #toSkype('Делаем разбивку для заливки','Airflow')
    to_load=[df.iloc[i*to_thread:(i+1)*to_thread,:] for i in range(df.shape[0]//to_thread+1)]
    
    x=datetime.datetime.now()
    #toSkype('Начинаем заливку в '+tablename,'Airflow')

    result=Parallel(n_jobs=n_jobs)(delayed(df_load_main)(dsn_database,tablename,df,columns,try_cnt) for df in to_load)
    y=datetime.datetime.now()
    #toSkype('Общее время загрузки -'+str(y-x),'Airflow')
    if show_time:
        print('Общее время загрузки -',str(y-x))
    #df_load_main(dsn_database,tablename,to_load[0],conn,columns)
    
    return df.shape[0]
        

    
def df_build(dsn_database,query,commands='',check_null=False,multi=False,show_time=True,params='',timeout=1200,dim_cycle='',list_cycle=[],step_cycle=700,hash=False,country='ru',try_cnt=10): # таймаут по умолчанию - 20 минут
    
    import sys
    import traceback
    
    import pytz
    tz = pytz.timezone('Asia/Irkutsk')

    dsn_database=dsn_database+('_'+country if country!='ru' else '')
    dsn_database_orig=dsn_database+''
    
    
    extra=''    
    
    
    hash_delta=0
    if hash:
        tz = pytz.timezone('Asia/Irkutsk')
        tablename=query.replace('.sql','')#+('_'+country if country!='ru' else '')
        dt=df_build('delta_bi',f"select coalesce(max(dt),'1999-01-01' ) dt from hash_tables.{tablename} where country='{country}'",show_time=False)
        hash_dt=dt.dt[0]
        hash_delta=(datetime.datetime.now(tz)-tz.localize(hash_dt)).total_seconds()//3600
        if hash_delta>1:
            # хэш старый, надо обновить
            # пусть процедура отработает, результат загрузим как новый хэш
            df_build('delta_bi','select 1 cnt',f"delete from hash_tables.{tablename} where country='{country}'")
                
        elif hash_delta==0 or hash_delta==1:
            # лучший вариант. хэш свежий, сразу берём
            table=df_build('delta_bi',f"select * from hash_tables.{tablename} where country='{country}'")
            table=table.drop(columns=['dt','country'])
            print('взято из кэша')
            return table
        elif hash_delta<0:
            # если стоит будущая дата, значит таблица заливается
            # потом придумаю, что делать. пока pass
            pass
    
    # берём логины/пароли
    # чтобы общий скрипт на свой комп и airflow

    try:
        from connections import conn
        dsn_hostname = conn[dsn_database]['dsn_hostname']
        dsn_port = conn[dsn_database]['dsn_port']
        dsn_uid = conn[dsn_database]['dsn_uid']
        dsn_pwd = conn[dsn_database]['dsn_pwd']
        db_type = conn[dsn_database]['db_type']
        try:
            dsn_database = conn[dsn_database]['dsn_database']
        except:
            pass
        try:
            extra=conn[dsn_database]['extra']
        except:
            pass
    except ModuleNotFoundError:
        from airflow.hooks.base import BaseHook
        connection = BaseHook.get_connection(dsn_database)
        dsn_hostname = connection.host
        dsn_port = connection.port
        dsn_uid = connection.login
        dsn_pwd = connection.password
        db_type = connection.description
        try:
            temp=connection.schema
            if str(temp)!='': # т.к. обычно совпадает с именем соединения, поэтому не прописываю
                dsn_database = connection.schema
        #print('Нет такого модуля')
        except:
            pass
        try:
            extra=connection.extra
        except:
            pass
    

    # чтобы вводить имена sql-файлов без расширения
    try:
        directory=os.getcwd()
    except:
        directory='/opt/airflow/dags/repo/dags'
        
    try:
        if not query.endswith('.sql') and query+'.sql' in os.listdir(directory):
            query=query+'.sql'
    except:
        pass
    
    time_begin=datetime.datetime.now(tz)
            
    if show_time and len(params)==0:
        print(query.split('\n')[0].replace('--',''),'-',end=' ') # выводим либо имя sql-скрипта, либо первую строку скрипта (убирая знаки коммента)
    
    # если аргументом идёт имя скрипта, то сразу его считываем
    try:
        if query.endswith('.sql'):
            query=readfile(directory+'/'+query) 
    except:
        toSkype('не могу считать файл '+query,'A')
        pass
        
    #if 'ProductFixesByTypeRecommendationsTotal' in query:
    #    toSkype('Найден Total','Airflow')
    
    if 'select' not in query.lower() and query!='':
        commands=query
        query=''

    if query=='':
        query='select 1 cnt'
        show_time=False
    
    # если есть параметры (подставляемые в скрипт переменные), то заменяем
    if len(params)>0:
        for var in params.keys():
            if isinstance(params[var],pd.core.frame.Series):
                params[var]=",".join("'"+params[var]+"'")
            elif isinstance(params[var],list):
                params[var]=",".join(["'"+x+"'" for x in params[var]])
            elif isinstance(params[var],int):
                params[var]=str(params[var])
                
            query=query.replace(var,str(params[var]))
            commands=commands.replace(var,str(params[var]))
            
            
    #print(query)
    
    
    #if country=='kz': # вроде поправили
    #    query=query.replace('product_FACT_TransitStock','product_FACT_TransitStockView')
    #    query=query.replace('product_FACT_Stock','product_FACT_StockView')
            
    # выводим либо имя sql-скрипта, либо первую строку скрипта (убирая знаки коммента)
    if show_time and len(params)>0:
        print(query.split('\n')[0].replace('--',''),'-',end=' ') 
    
    if commands.endswith('.sql'):
        commands=readfile(commands)
    
    table=[]

    # если выгружаем по складам, то делаем здесь же
    if dim_cycle=='@StoreCode' or dim_cycle=='@StoreID':
        print()
        del table
        branches=df_build('dns_dwh','branches_dnsdwh',show_time=False,country=country)
        stores=branches.query('type in ["Магазин","Дисконт центр"]')[dim_cycle.replace('@','')].drop_duplicates().sort_values()
        
        for store in stores:
            temp=df_build(dsn_database,query,params={dim_cycle:str(store)},multi=multi,country=country,check_null=check_null) 

            if 'table' not in dir():
                table=temp.copy()
            else:
                # если есть - дополняем
                table=table.append(temp)
        time_end=datetime.datetime.now(tz)
        if show_time==True:
            print()
            print('Итого выгрузка - '+str(time_end-time_begin))
    elif dim_cycle!='':
        del table
        #print(query)
        print()
        for i in range((list_cycle.shape[0]-1)//step_cycle+1):
            
            list_cycle_str=",".join("'"+list_cycle.iloc[i*step_cycle:(i+1)*step_cycle]+"'")
            #print(list_cycle_str)
            # проверяем цикловые таблицы на ненулёвость
            temp=df_build(dsn_database,query,params={dim_cycle:list_cycle_str},multi=multi,country=country,check_null=check_null)
            try:
                temp=temp.query('branch_guid!="xxx"') # костыль на случай, если результатов нет, а на пустую таблицу проверка есть
            except:
                pass

            if 'table' not in dir():
                table=temp.copy()
            else:
                # если есть - дополняем
                table=table.append(temp)
        time_end=datetime.datetime.now(tz)
        if show_time==True:
            print()
            print('Итого выгрузка - '+str(time_end-time_begin))

    #elif dsn_database in ['dns_log','mart_fois'] or dsn_database_orig in ['adm_fcs','adm_fcs4','dns_log_test','vlad']:
    elif db_type=='clickhouse':
        
 
        import clickhouse_driver as cd
       
        if dsn_database_orig=='vlad':
            client = cd.Client(database=dsn_database, host=dsn_hostname, 
                    settings=dict(numpy_columns=True),sync_request_timeout=5)   #port=dsn_port,   
        else:
                client = cd.Client(database=dsn_database, user=dsn_uid, 
                    password=dsn_pwd, host=dsn_hostname,
                    settings=dict(numpy_columns=True),sync_request_timeout=5)
        if commands!='':
            client.execute(commands)
            
            scr_check="""
            select count(*) cnt
            from system.mutations
            where 1=1
                and table='@tablename'
                and is_done<>1
                and command like 'DELETE%'
            """
            
                                     
        cnt=0
        while cnt<=13:
            cnt+=1
            try:
                
                itog_temp,cols= client.execute(query,with_column_types=True)
                if check_null and len(itog_temp)==0:
                    raise Exception('Не отработал запрос в базе ' + dsn_database) # этим уходим в except и пробуем ещё раз
            except Exception as e:
                print(cnt,'КликХаус прилёг отдохнуть')
                print(str(e.__class__.__name__))
                print(str(sys.exc_info()[1]))
                #print(str(traceback.format_exc()))
                toSkype(f'Ошибка в цикле загрузки кликхауса {cnt}\n '+str(traceback.format_exc())+query[:1000],chat='Airflow')
                sleep(2*cnt)
            else:
                # если except не отработал, значит попытка успешная, и выходим из цикла
                break
 
        if 'itog_temp' in locals():
            table=pd.DataFrame(list(itog_temp),columns=[desc[0] for desc in cols])
            client.disconnect()
        else:
            client.disconnect()
            raise Exception('Не отработал запрос в базе ' + dsn_database)
 
    #elif dsn_database in ['mart_com','mart_fls','delta_bi','dep_spb','ORP_base'] or dsn_database_orig in ['startml','ORP_base']:
    elif db_type=='postgre':
        #print('xxx')
        import psycopg2
        # dbname=dsn_database, 
        # options=f'-c statement_timeout={str(timeout)}000') # 20 минут

        conn = psycopg2.connect(dbname=dsn_database, user=dsn_uid, 
                                password=dsn_pwd, host=dsn_hostname,
                                options=f"-c search_path={extra}")

                                
        cursor = conn.cursor()
        
        if commands!='':
            cnt=0
            while cnt<=try_cnt:
                cnt+=1
                try:
                    cursor.execute(commands)
                    #toSkype(f'Успех в цикле загрузки постгреса {cnt}',chat='Airflow')
                except Exception as e:
                    sleep(5)
                    #toSkype(f'Ошибка в цикле загрузки постгреса {cnt}\n '+str(traceback.format_exc())+query[:1000],chat='Airflow')
                else:
                    break
            
            conn.commit()

        cursor.execute(query)
        itog_temp = cursor.fetchall()
        table=pd.DataFrame(list(itog_temp),columns=[desc[0] for desc in cursor.description])

        cursor.close()
        conn.close()
    elif dsn_database=='dns_log2':
        import clickhouse_driver as cd
        
        dsn_hostname = "10.0.75.197"
        dsn_port = "8123" #
        dsn_uid = ""
        dsn_pwd = ""
        client = cd.Client(database='dns_log', host=dsn_hostname,
                                #user=dsn_uid, password=dsn_pwd, 
                                settings=dict(numpy_columns=True),sync_request_timeout=5) 
        itog_temp,cols= client.execute(query,with_column_types=True)
        table=pd.DataFrame(list(itog_temp),columns=[desc[0] for desc in cols])
        client.disconnect()

    #elif dsn_database in ['cdc_current_state','bi_fls','bi_fois']:
    elif db_type=='greenplum':
        import psycopg2
        
        conn = psycopg2.connect(dbname=dsn_database, user=dsn_uid, 
                                password=dsn_pwd, host=dsn_hostname,
                                options='-c statement_timeout=5400000') # 5400000 таймаут - полтора часа. вводится в миллисекундах (т.е. секунды и три нуля в конце). Ввожу из-за просмотры-продажи
        cursor = conn.cursor()
        
        if commands!='':
            cursor.execute(commands)
            
        cursor.execute(query)
        itog_temp = cursor.fetchall()
        table=pd.DataFrame(list(itog_temp),columns=[desc[0] for desc in cursor.description])
        
        cursor.close()
        conn.close()
    #elif dsn_database=='dns_dwh' and multi==False:
    elif db_type=='mssql' and multi==False:
        import pymssql 
        #import pyodbc
     
        #conn = pyodbc.connect(r'Driver=SQL Server;Server=adm-sql-dwh.partner.ru;Database=dns_dwh;Trusted_Connection=yes;') # полностью рабочее
        conn = pymssql.connect(server=dsn_hostname,\
                         user=dsn_uid, password=dsn_pwd, database=dsn_database)

        #conn = pymssql.connect(server='adm-sql-dwh.partner.ru', database='dns_dwh',trusted=True) 
        
        #cursor = conn.cursor()

        cnt=0
        while cnt<=try_cnt:
            cnt+=1
            try:
                table = pd.read_sql(query, conn)
                if isinstance(table,list):
                    raise Exception('ошибочка')
            except Exception as e:
                sleep(5)
            else:
                break

        
        
        #cursor.close()
        conn.close()
    #elif dsn_database=='dns_dwh' and multi==True:
    elif db_type=='mssql' and multi==True:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session
        
        #engine = create_engine('mssql+pyodbc://adm-sql-dwh.partner.ru/dns_dwh?driver=SQL Server?Trusted_Connection=yes') # полностью рабочее
        engine = create_engine(f'mssql+pymssql://{dsn_uid}:{dsn_pwd}@{dsn_hostname}/{dsn_database}')
        conn = engine.connect()
        scripts=query.split(';')
        for scr in scripts[:-1]:
            #print(scr)
            conn.execute(scr)
        #print(scripts[-1])
        result=conn.execute(scripts[-1])
        table=pd.DataFrame(result.fetchall(),columns=result.keys())
        conn.close()
    elif dsn_database=='dns_dwh_vs':
        import psycopg2
        
        dsn_hostname = "2.60.113.206"
        #dsn_port = "5432" # ????
        dsn_uid = "valery_programmer"
        dsn_pwd = "1234567890"
        conn = psycopg2.connect(dbname=dsn_database, user=dsn_uid, 
                                password=dsn_pwd, host=dsn_hostname)
        cursor = conn.cursor()
        
        if commands!='':
            cursor.execute(commands)
            conn.commit()
        
        if query!='':
            cursor.execute(query)
            itog_temp = cursor.fetchall()
            table=pd.DataFrame(list(itog_temp),columns=[desc[0] for desc in cursor.description])

        cursor.close()
        conn.close()

    elif dsn_database=='dns_m':
        import pymssql 
        #import pyodbc
        from func_timeout import func_timeout, FunctionTimedOut
        
        #conn = pyodbc.connect(r'Driver=SQL Server;Server=adm-sql-s1.partner.ru;Database=dns_m;Trusted_Connection=yes;') # полностью рабочий
        
        conn = pymssql.connect(server=dsn_hostname,\
                         user=dsn_uid, password=dsn_pwd, database=dsn_database)
                         
        #conn = pymssql.connect(server='adm-sql-s1.partner.ru', database=dsn_database,timeout=2) 
        cursor = conn.cursor()
        try:
            table = func_timeout(timeout, pd.read_sql, args=(query, conn))
            #table = pd.read_sql(query, conn)
        except FunctionTimedOut:
            raise Exception('Сработал таймаут для ' + dsn_database)
        except Exception as e:
            raise Exception('Ошибка при выгрузке запроса из ' + dsn_database)
        
        cursor.close()
        conn.close()
    
        
    if table is None:
        raise Exception('Ничего не выгрузилось, ' + dsn_database)
        
    elif isinstance(table,list):
        if table==[] and check_null:
            #print(query)
            raise Exception('Выгрузилась пустая таблица из ' + dsn_database)
        else:
            #print(query)
            toSkype('x'+str(table)+'x','A')
            raise Exception('Выгрузился список из ' + dsn_database)    
        
    elif table.empty and check_null:
        #print(query)
        raise Exception('Выгрузилась пустая таблица из ' + dsn_database)
    
    
    time_end=datetime.datetime.now(tz)
    if show_time:
        print(str(time_end-time_begin)+ ' ' +str(table.shape[0]))
    
    if hash and hash_delta>1: # вообще если грузится из хэша, то сверху сразу делается return, но перестрахуюсь
        tz = pytz.timezone('Asia/Irkutsk')
        print('Сейчас загрузим в хэш')
        actual_dt=datetime.datetime.now(tz)
        table['dt']=actual_dt
        table['country']=country
        time_begin= datetime.datetime.now(tz)

        df_load('delta_bi',f'hash_tables.{tablename}',table)
        time_end= datetime.datetime.now(tz)
        print('Загружено в хэш - '+str(time_end-time_begin))
        toSkype(str(actual_dt)+f'\n загружено в хэш {country}\n'+tablename,'Airflow')
    
    return table
    

    
    
def timemark(filename):

    dt=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(filename, "a") as f:
        f.write( dt+';')
    
def timemark2(filename,task_uuid,typ,value=''):
    #import os.path
    if task_uuid=='':
        return 0
    if value=='':
        value=datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    msg=task_uuid+';'+typ+';'+value+';'
    
    if os.path.exists(filename+'x'):
        with open(filename, "a") as f:
            #f.write('\n'+task_uuid+';')
            #f.write(typ+';')
            #f.write(dt+';')
            f.write('\n'+msg)
    else:
        #toSkype(msg,'Airflow')
        s=f"""
        insert into etl.log (task_guid,value_type,value)
        values ('{task_uuid}','{typ}','{value}')
        """
        df_build('delta_bi','select 1 cnt',s)
    
def to_csv(df,name=''):#,new_version=False
    # если не задаём имя файла, то берётся название сохраняемого датафрейма
    if name=='':
        name='temp_'+datetime.datetime.now(tz).strftime("%Y-%m-%d %H-%M-%S")
    #if new_version:
    #    if os.path.isfile(name+'.csv'):
    #        name=name+'_'+datetime.datetime.now(tz).strftime("%Y-%m-%d %H-%M-%S")
    df.to_csv(name+'.csv',sep=';',encoding='utf-8-sig', float_format="%.8f",decimal=',',index=False)


def to_sql(df,tablename='',n_jobs=4):
    if tablename=='':
        tablename='temp_'+datetime.datetime.now(tz).strftime("%Y_%m_%d_%H_%M_%S")
        
    scr_check=f"""
     select *
     from INFORMATION_SCHEMA.tables
     where table_schema ='temp'
     and table_name='{tablename}'
    """
    
    scr_delete=f"""
    drop table temp.{tablename}
    """
    

        
    s="CREATE TABLE temp."+tablename+ ' (\n'
    
    for col in df.columns:
        if df[col].dtypes==object:
            s+='"'+col+'" '+'varchar,\n'
        elif 'int' in str(df[col].dtypes):
            s+='"'+col+'" '+'float8,\n'
        elif 'float' in str(df[col].dtypes):
            s+='"'+col+'" '+'float8,\n'
        elif 'ecimal' in str(df[col].dtypes):
            s+='"'+col+'" '+'decimal,\n'
        elif 'date' in str(df[col].dtypes):
            s+='"'+col+'" '+'timestamp,\n'
            df[col]=df[col].fillna('1900-01-01')
    s=s[:-2]+')'

        
    check=df_build('delta_bi',scr_check)
    sleep(3)
    if check.empty: # т.е. такой таблицы нет
        #df_build('delta_bi','select 1 cnt',scr_delete)
        #toSkype('удалена таблица '+tablename,'Airflow')
        # создаём
        df_build('delta_bi','select 1 cnt',s)
        
    else: # если есть
        df_build('delta_bi','select 1 cnt',scr_delete)
        sleep(4)
        df_build('delta_bi','select 1 cnt',s)
    print(s)
    # заливаем
    df_load('delta_bi','temp.'+tablename,df,n_jobs=n_jobs)
    
    return tablename
    
def log(dt='',guid=''):
    """
    from my_functions import *
    logg=pd.read_csv('log_new.txt',names=(['guid','action','msg']),sep=';', encoding='ANSI',header= None,index_col=False)
    tasks=logg[(logg.msg.apply(lambda x:str(x).startswith('2022-02-25'))) & (logg.action=='Начало')].guid
    logg=logg.merge(tasks)
    tasks=logg[(logg.action=='Доля успешной загрузки')&(logg.msg.apply(lambda x:str(x).startswith('100')))].guid
    logg[(logg.action=='Отправлено')].msg.astype('int').sum()
    """
    logg=pd.read_csv('log.txt',names=(['guid','dt_begin','dt_kafka','dt_end','cnt_all','cut_success','msg','comm']),sep=';', encoding='ANSI',header= None)
    logg.cut_success=logg.cut_success.apply(lambda x: x if str(x).replace('.','').isnumeric() else '0')
    logg.cut_success=logg.cut_success.astype(float)
    logg.cnt_all=logg.cnt_all.astype(float)
    if dt!='':
        logg=logg[logg.dt_begin.apply(lambda x: str(x).startswith(dt))]
    if guid!='':
        logg=logg[logg.guid==guid]
    logg.msg=logg.msg.replace(',','\n')
    return logg
    
   
def execution(ModuleName,params='',check_kafka=True):
    """
    запуск фиксаций через название модуля
    отправка информационных сообщений в чат телеграма
    """
    exec(f'import {ModuleName} as main_module',globals())
    try:
        
        #from my_functions import toSkype
        status='bad'
        
        country='ru'
        try:
            country=params['country']
        except:
            pass
            
        cnt=1
        msg_agg=''
        
        print(main_module.__doc__)
        
        while status!='ok' and cnt<=3: # делаем три попытки вычислить фиксацию и получить статус ok
            print("Попытка",cnt)
            if params=='':
                status,msg=main_module.main()
            else:
                status,msg=main_module.main(params)
            print(status)
            print(msg)
            print()
            msg_str=str(msg).split('\n')
            # обрезаем слишком длинные сообщения об ошибках. Например, когда цитируется весь SQL-скрипт
            if len(msg_str)>4:
                msg_str='\n'.join(msg_str[:2])+'\n'+'\n'.join(msg_str[-2:])
            else:
                msg_str=str(msg)
            cnt+=1
            msg_agg+=msg_str+'\n'
        
        if status!='ok' and cnt>3: # если после трёх попыток не получили статус ok - отправляем ошибку
            print('Неустранимая ошибка', main_module.__doc__,msg)
            toSkype(datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')+' ирк\n'+'Неустранимая ошибка\n' + main_module.__doc__ + '\n'+((str.upper(country)+'\n\n') if country!='ru' else '') + msg_agg)
            toSkype(main_module.__doc__ + '\n' +((str.upper(country)+'\n\n') if country!='ru' else '') + msg_agg,chat='Увед')
        elif status=='ok': # если получили статус ok - отправляем сообщение об успешном исполнении
            toSkype(datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')+' ирк\n'+main_module.__doc__ + '\n'+((str.upper(country)+'\n\n') if country!='ru' else '') + msg)
            if (not '100.0' in msg) and check_kafka: toSkype(main_module.__doc__ + '\n' + msg +'\nДоля доставленных в Кафку строк отличается от 100',chat='Увед') # дополнительный контроль на полноту доставки в Кафку
    except Exception as e:
        print(str(e.__class__.__name__))
        print(str(sys.exc_info()[1]))
        print(str(traceback.format_exc()))
        return str(sys.exc_info()[1])
    print()
    
layer_guids_old="""
		'9a4ba750-fe2a-11eb-a2a2-00155dfc8232',
		'8868eba7-3300-11ec-8f0f-00155d8ed20b',
		'6dfff91b-3313-11ec-8f10-00155d8ed20c',
		'e102b4df-3c53-11ec-8f20-00155d8ed20b',
		'b130dd9f-4819-11ec-8f31-00155d8ed20b',
		'8eb9bbe7-5328-11ec-8f3e-00155d8ed20b',
		'0858be7a-5720-11ec-8f42-00155d8ed20b',
        '65494641-7fe8-11ec-8f69-00155d8ed20b',
        'c75551b3-83ea-11ec-8f6c-00155d8ed20b',
        '182da7c7-8b00-11ec-8f75-00155d8ed20c',
        'e23c19a4-8b41-11ec-8f75-00155d8ed20c',
        '2be4aef3-3550-11ec-8f12-00155d8ed20c',
        'c77157b5-436d-11ec-8f2b-00155d8ed20b',
        '7136e3fd-99c0-11ec-8f82-00155d8ed20c',
        '8d66b8e1-99e4-11ec-8f81-00155d8ed20b',
        '4ebe8ef3-9c10-11ec-8f84-00155d8ed20c',
        '6d6c0832-9c22-11ec-8f83-00155d8ed20b',
        'fd829d44-b93a-11ec-8fa1-00155d8ed20c'
        """
        
layer_guids_old2="""
		'9a4ba750-fe2a-11eb-a2a2-00155dfc8232',
        '8868eba7-3300-11ec-8f0f-00155d8ed20b',
        '6dfff91b-3313-11ec-8f10-00155d8ed20c',
        '2be4aef3-3550-11ec-8f12-00155d8ed20c',
        'e102b4df-3c53-11ec-8f20-00155d8ed20b',
        'b130dd9f-4819-11ec-8f31-00155d8ed20b',
        '8eb9bbe7-5328-11ec-8f3e-00155d8ed20b',
        '0858be7a-5720-11ec-8f42-00155d8ed20b',
        '65494641-7fe8-11ec-8f69-00155d8ed20b',
        '182da7c7-8b00-11ec-8f75-00155d8ed20c',
        'e23c19a4-8b41-11ec-8f75-00155d8ed20c',
        '7136e3fd-99c0-11ec-8f82-00155d8ed20c',
        'fd829d44-b93a-11ec-8fa1-00155d8ed20c',
        '8d8c3e9e-0532-11ec-a2aa-00155dfc8232',
        'd0bca287-ea8f-11eb-a292-00155dbd7634',
        '024826cd-f3e9-11eb-a29a-00155dfc8232',
        '08f0de26-0084-11ec-a2a7-00155dbd7634',
        '4739b57e-f34d-11eb-a29b-00155dbd7634',
        '8ddce976-1837-11ec-8eed-00155d8ed20b',
        'edf8ad55-1b42-11ec-8eef-00155d8ed20b',
        '3078ca9e-1b4f-11ec-8eef-00155d8ed20b',
        '6e258b22-9373-11ec-8f7b-00155d8ed20b',
        '68cbf519-a334-11ec-8f89-00155d8ed20b',
        'e47fc48f-bee6-11ec-8fa7-00155d8ed20c',
        'b118969b-8ebf-11ec-8f77-00155d8ed20c',
        '14e85f7e-cd18-11ec-8fb6-00155d8ed20c',
        '821aeadc-ec66-11ec-8fd5-00155d8ed20c'
        """


layer_guids_old333="""
		'8868eba7-3300-11ec-8f0f-00155d8ed20b',
        '6dfff91b-3313-11ec-8f10-00155d8ed20c',
        '0858be7a-5720-11ec-8f42-00155d8ed20b',
        'b118969b-8ebf-11ec-8f77-00155d8ed20c',
        '6e258b22-9373-11ec-8f7b-00155d8ed20b',
        '7136e3fd-99c0-11ec-8f82-00155d8ed20c',
        '68cbf519-a334-11ec-8f89-00155d8ed20b',
        '14e85f7e-cd18-11ec-8fb6-00155d8ed20c',
        '821aeadc-ec66-11ec-8fd5-00155d8ed20c',
        '43cc7b74-f76c-11ec-8fe0-00155d8ed20c',
        'e102b4df-3c53-11ec-8f20-00155d8ed20b',
        'b130dd9f-4819-11ec-8f31-00155d8ed20b',
        '8eb9bbe7-5328-11ec-8f3e-00155d8ed20b',
        '65494641-7fe8-11ec-8f69-00155d8ed20b',
        '182da7c7-8b00-11ec-8f75-00155d8ed20c',
        'e23c19a4-8b41-11ec-8f75-00155d8ed20c',
        'd0bca287-ea8f-11eb-a292-00155dbd7634',
        '9a4ba750-fe2a-11eb-a2a2-00155dfc8232'
        """



# списки гуидов, которые не нужно занулять. Это сами зануляторы + оффлайн-коэффициенты
solid_not_layer_guids="""
        'e8743a1f-84b5-11ec-8f6d-00155d8ed20b',
        'decdd0d6-8aec-11ec-8f74-00155d8ed20b',
        'fe3941dd-79c0-11ec-8f64-00155d8ed20c',
        'd0f607a0-7433-11ec-8f5e-00155d8ed20c',
        'ccbb1347-5e42-11ec-8f49-00155d8ed20b',
        '2184ebac-74cb-11ec-8f5f-00155d8ed20b',
        '0104d92d-74db-11ec-8f5f-00155d8ed20b',
        'Зануление старше трёх месяцев',
        '24cae7e9-3550-11ec-8f12-00155d8ed20c',
        '36e7c8e0-fbef-11eb-a2a3-00155dbd7634',
        'abad7082-15f3-11ec-8eec-00155d8ed20c',
        'b1e414d6-4c11-11ec-8f34-00155d8ed20b',
        '0ab41161-5bac-11ec-8f46-00155d8ed20b',
        '27141f8f-737f-11ec-8f5d-00155d8ed20c',
        '6a49b46b-d7e8-11ec-8fbf-00155d8ed20b',
        'aeb92c9f-e2f1-11ec-8fcc-00155d8ed20c',
        '798bc205-e30b-11ec-8fcb-00155d8ed20b',
        '34292ca4-e53d-11ec-8fcc-00155d8ed20b',
        '220f599d-e54c-11ec-8fcc-00155d8ed20b',
        'ddb1a1e2-ec61-11ec-8fd5-00155d8ed20c',
        
        'b130dd9f-4819-11ec-8f31-00155d8ed20b',
        'e102b4df-3c53-11ec-8f20-00155d8ed20b',
        '9a4ba750-fe2a-11eb-a2a2-00155dfc8232',
        '8eb9bbe7-5328-11ec-8f3e-00155d8ed20b',
        '65494641-7fe8-11ec-8f69-00155d8ed20b',
        'fd829d44-b93a-11ec-8fa1-00155d8ed20c',
        '9a564efa-0d51-11ed-8ff5-00155d8ed20c'
        """  

layer_guids_big="""
        'abad7082-15f3-11ec-8eec-00155d8ed20c',
        '36e7c8e0-fbef-11eb-a2a3-00155dbd7634',
        '9a4ba750-fe2a-11eb-a2a2-00155dfc8232',
        '8868eba7-3300-11ec-8f0f-00155d8ed20b',
        '6dfff91b-3313-11ec-8f10-00155d8ed20c',
        'e102b4df-3c53-11ec-8f20-00155d8ed20b',
        'b130dd9f-4819-11ec-8f31-00155d8ed20b',
        'b1e414d6-4c11-11ec-8f34-00155d8ed20b',
        '8eb9bbe7-5328-11ec-8f3e-00155d8ed20b',
        '0858be7a-5720-11ec-8f42-00155d8ed20b',
        '27141f8f-737f-11ec-8f5d-00155d8ed20c',
        '3af98acc-f34d-11eb-a29b-00155dbd7634',
        '9740e59f-03b1-11ec-a2a7-00155dfc8232',
        '8d8c3e9e-0532-11ec-a2aa-00155dfc8232',
        'd0bca287-ea8f-11eb-a292-00155dbd7634',
        '024826cd-f3e9-11eb-a29a-00155dfc8232',
        '08f0de26-0084-11ec-a2a7-00155dbd7634',
        '345787f5-f34d-11eb-a29b-00155dbd7634',
        '4739b57e-f34d-11eb-a29b-00155dbd7634',
        '8ddce976-1837-11ec-8eed-00155d8ed20b',
        'edf8ad55-1b42-11ec-8eef-00155d8ed20b',
        '3078ca9e-1b4f-11ec-8eef-00155d8ed20b',
        '45a36863-1ffa-11ec-8ef6-00155d8ed20c'
        """
    
hand_guids_old="""
        'e5dbdc9b-f3e8-11eb-a29a-00155dfc8232',
        'a8438be5-a6e3-11eb-a255-00155dd2ff18',
        'aec6a5d2-a6e3-11eb-a255-00155dd2ff18',
        'd5a41c7c-f3e8-11eb-a29a-00155dfc8232',
        '2c08b108-a3eb-11eb-a253-00155d42eb15'
        """


def layer_guids():
    try:
        layer_guids_df=df_build('dep_spb',"""select guid from delta_bi.etl_calculus where layer_or_hand='layer'""",show_time=False)
        return layer_guids_df#",".join("'"+layer_guids_df.guid+"'")
    except:
        toSkype(str(traceback.format_exc()),'Airflow')

def hand_guids():
    try:
        hand_guids_df=df_build('dep_spb',"""select guid from delta_bi.etl_calculus where layer_or_hand='hand'""",show_time=False)
        return ",".join("'"+hand_guids_df.guid+"'")
    except:
        toSkype(str(traceback.format_exc()),'Airflow')

smart_guids="""
        'e5dbdc9b-f3e8-11eb-a29a-00155dfc8232',
        'd5a41c7c-f3e8-11eb-a29a-00155dfc8232',
        'abad7082-15f3-11ec-8eec-00155d8ed20c',
        '9a4ba750-fe2a-11eb-a2a2-00155dfc8232',
        '8868eba7-3300-11ec-8f0f-00155d8ed20b',
        '6dfff91b-3313-11ec-8f10-00155d8ed20c',
        'e102b4df-3c53-11ec-8f20-00155d8ed20b',
        'b130dd9f-4819-11ec-8f31-00155d8ed20b',
        '8eb9bbe7-5328-11ec-8f3e-00155d8ed20b',
        '9740e59f-03b1-11ec-a2a7-00155dfc8232',
        'd0bca287-ea8f-11eb-a292-00155dbd7634',
        '024826cd-f3e9-11eb-a29a-00155dfc8232',
        '08f0de26-0084-11ec-a2a7-00155dbd7634',
        '6e258b22-9373-11ec-8f7b-00155d8ed20b',
        '0858be7a-5720-11ec-8f42-00155d8ed20b',
        '65494641-7fe8-11ec-8f69-00155d8ed20b',
        '182da7c7-8b00-11ec-8f75-00155d8ed20c',
        'e23c19a4-8b41-11ec-8f75-00155d8ed20c'
        """

correlated_guids_todelete="""
        '8868eba7-3300-11ec-8f0f-00155d8ed20b',
        '6dfff91b-3313-11ec-8f10-00155d8ed20c',
        'e102b4df-3c53-11ec-8f20-00155d8ed20b',
        'c77157b5-436d-11ec-8f2b-00155d8ed20b',
        'b130dd9f-4819-11ec-8f31-00155d8ed20b',
        '0858be7a-5720-11ec-8f42-00155d8ed20b',
        '6e258b22-9373-11ec-8f7b-00155d8ed20b',
        '68cbf519-a334-11ec-8f89-00155d8ed20b',
        '024826cd-f3e9-11eb-a29a-00155dfc8232',
        '345787f5-f34d-11eb-a29b-00155dbd7634',
        '9a4ba750-fe2a-11eb-a2a2-00155dfc8232'
        """

deletion_three_months_guids=[
    'abad7082-15f3-11ec-8eec-00155d8ed20c',
    '36e7c8e0-fbef-11eb-a2a3-00155dbd7634',
    '9a4ba750-fe2a-11eb-a2a2-00155dfc8232',
    '8868eba7-3300-11ec-8f0f-00155d8ed20b',
    '6dfff91b-3313-11ec-8f10-00155d8ed20c',
    'e102b4df-3c53-11ec-8f20-00155d8ed20b',
    'b130dd9f-4819-11ec-8f31-00155d8ed20b',
    '8eb9bbe7-5328-11ec-8f3e-00155d8ed20b',
    '0858be7a-5720-11ec-8f42-00155d8ed20b',
    '9740e59f-03b1-11ec-a2a7-00155dfc8232',
    '8d8c3e9e-0532-11ec-a2aa-00155dfc8232',
    'd0bca287-ea8f-11eb-a292-00155dbd7634',
    '024826cd-f3e9-11eb-a29a-00155dfc8232',
    '08f0de26-0084-11ec-a2a7-00155dbd7634',
    '4739b57e-f34d-11eb-a29b-00155dbd7634',
    '8ddce976-1837-11ec-8eed-00155d8ed20b',
    'edf8ad55-1b42-11ec-8eef-00155d8ed20b',
    '3078ca9e-1b4f-11ec-8eef-00155d8ed20b',
    'b118969b-8ebf-11ec-8f77-00155d8ed20c',
    'ccbb1347-5e42-11ec-8f49-00155d8ed20b',
    '2184ebac-74cb-11ec-8f5f-00155d8ed20b',
    '3aba9dae-2d74-11ec-8f07-00155d8ed20c',
    '94ec4966-87d4-11ec-8f71-00155d8ed20c',
    '1c451fbd-3545-11ec-8f11-00155d8ed20b',
    '0afbec46-531d-11ec-8f3e-00155d8ed20b',
    '32a02587-50c5-11ec-8f39-00155d8ed20b',
    '3069a5e9-7e3f-11ec-8f67-00155d8ed20b',
    '02e780f7-7e63-11ec-8f67-00155d8ed20b',
    '2a250a1f-7e60-11ec-8f67-00155d8ed20b',
    '7c82fc36-855c-11ec-8f6e-00155d8ed20b',
    'c004f189-8877-11ec-8f71-00155d8ed20b'
    ]
    
deletion_three_months_guids_test=[
    '6dfff91b-3313-11ec-8f10-00155d8ed20c',
    '3078ca9e-1b4f-11ec-8eef-00155d8ed20b'
    ]
    
DELETE_category_exclusion=[
        'ID11686','ID01209','ID01166','ID41319','EM06984','EM06700',
        'ID25062','ID38662','ID56353','ID09691','EY69002',
        'EY69063','EY68669','EM00969']

category_exclusion_negative=['AM18253','x']

def before_start(recomm_type,check_guid=True,country='ru'):

    try:
        # задаём рабочую папку
        os.chdir(os.path.dirname(__file__)) 

    
        # проверяем, чтобы рекомендация относилась к слоям, слоям-исключениям или ручным
        if recomm_type not in layer_guids() and recomm_type not in solid_not_layer_guids and recomm_type not in hand_guids() and check_guid:
            #msg=f"ВНИМАНИЕ: рекомендация {recomm_type} не указана в списке слоёв (для отрицательных фиксаций) или исключений"
            #toSkype(msg,chat='Увед')
            pass # перевожу в БД, можно следить и так
        
        # создаём гуид для логирования
        import uuid
        task_uuid=str(uuid.uuid1())
        

        
        # загружаем стандартные словари
        x=datetime.datetime.now()
        branches=df_build("dns_dwh",'branches_dnsdwh',timeout=60,check_null=True,country=country)
        y=datetime.datetime.now()
        yx=(y-x).seconds
        if yx<30:
            products=df_build("dns_dwh",'products_dnsdwh',timeout=300,check_null=True,country=country)
            cats=df_build("dns_dwh",'categories_dnsdwh',timeout=60,check_null=True,country=country)
        
        #from my_functions import timemark,timemark2
        
        # логируем гуид рекомендации
        #with open('log.txt', "a") as f:
        #    f.write('\n'+recomm_type+';')
        # логируем дату начала
        #timemark('log.txt')
        
        #with open('log_new.txt', "a") as f:
        #    f.write('\n'+task_uuid+';')
        #    f.write('ГУИД;')
        #    f.write(recomm_type+';')
            
        timemark2('log_new.txt',task_uuid,'ГУИД',recomm_type)    
        timemark2('log_new.txt',task_uuid,'Начало')
        if yx>=10:
            sleep(30)
            raise Exception('Серьёзное замедление в работе dns_dwh. Обновление рискованно')
        
        return task_uuid,cats,branches,products
        #return {'task_uuid':task_uuid,'cats':cats,'branches':branches,'products':products}#
    except:
        toSkype('before start\n'+str(traceback.format_exc())[:1500],'Airflow')
        
        
def before_start_spb(recomm_type,master):
    try:
        # задаём рабочую папку
        os.chdir(os.path.dirname(__file__)) 

        # создаём гуид для логирования
        #CR по импорту тоже вынести в начало файла
        #CR_answer вынес
        #import uuid
        task_uuid=str(uuid.uuid1())


        #CR решение вытащить файл выглядит повторяющимся, я бы вынес в отдельню функцию оставив код вида:
        # branches = master.read_table(read_sql_from_file('branches_spb.sql'))
        #CR_answer вынес в отдельную функцию

        #with open('branches_spb.sql',encoding='utf-8-sig',mode='r') as f:
        #    branches = master.read_table(f.read())
        branches = master.read_table(readfile('branches_spb.sql'))

        #with open('products_spb.sql',encoding='utf-8-sig',mode='r') as f:
        #    products = master.read_table(f.read())
        products = master.read_table(readfile('products_spb.sql'))
            
        #with open('categories_spb.sql',encoding='utf-8-sig',mode='r') as f:
        #    cats = master.read_table(f.read())
        cats = master.read_table(readfile('categories_spb.sql'))

        #CR вместо timemark2 альтернативно можно использовать штатные средства логирования, если у нас потребность записи в файл.
        # внутри логика которая может влетать на функцию df_load
        #CR_answer сейчас лог ведётся в базе, вставляется построчно, так и задумывалось. df_load внутри не используется, инструкция вставки формируется вручную
        # какие штатные средства логирования необходимо использовать?

        timemark2('log_new.txt',task_uuid,'ГУИД',recomm_type)    
        timemark2('log_new.txt',task_uuid,'Начало')
         
        return task_uuid,cats,branches,products
    except:
        toSkype('before start\n'+str(traceback.format_exc())[:1500],'Airflow')
    
json_functions={'ProductFixesByTypeRecommendations':'load_fixed',
                'ProductFixesByTypeRecommendationsNew':'load_fixed',
                'ProductExposure_PrioritizationCoefficients':'load_offline_coeff',
                'SalesPotentialsByTypeOfRecommendation':'load_sales_potential',
                'SalesForecastByTypeRecommendations':'load_SalesForecast',
                'sales_forecast_full.dns_dvvs.dv':'load_forecast_weekly',
                'sales_forecast_m_full.dns_dvvs.dv':'load_forecast_monthly',
                'SalesPlanForProductWarehouseTotal.dns_dvvs.dv':'load_forecast_decomposed_final',
                'SalesPlanForProductWarehouseTotal1C.dns_dvvs.dv':'load_forecast_decomposed_final',
                'Sales_Forecast_Warehouse_Full':'load_forecast_rrc_weekly',
                'ActualTurnoverCategoryForPotentials.dv':'load_turnover',
                'CreationOfDocumentsForDistribution':'load_CreationOfDocumentsForDistribution',
                'AvarageDailySales':'load_potential_PP',
                'AdditionalPrioritizationCoefficients':'load_AdditionalPrioritizationCoefficients',
                'DimAutoSegments':'load_autosegmets_segments',
                'ProductAutoSegments':'load_autosegmets_products',
                'SalesForecastTurnover':'load_pp'}
old_ibd="""
# сопоставление названий ИБД и их суффиксов
IBD_topics=pd.DataFrame([['Верхне-Волжский (Казань)',
                'Дальний Восток и Восточная Сибирь',
                'Центральный р. (Москва)',
                'Приволжский (Нижний Новгород)',
                'Средне-Волжский (Самара)',
                'Сибирь (Новосибирск)',
                'Урал (Екатеринбург)',
                'Черноземье (Воронеж)',
                'Южный (Ростов)'],
                ['.dns_uv.dv',
                '.dns_dvvs.dv',
                '.dns_msk.dv',
                '.dns_prv.dv',
                '.dns_mv.dv',
                '.dns_sib.dv',
                '.dns_url.dv',
                '.dns_chrz.dv',
                '.dns_sth.dv']]).T
IBD_topics.columns=['IBDname','topic_suffix']
"""

# по какой колонке проверяем тип рекомендаций при выгрузке действующих. Для push_kafka
recomm_column={'ProductFixesByTypeRecommendations':'main.TypeRecommendationsID',
                'ProductFixesByTypeRecommendationsNew':'main.TypeRecommendationsID',
                'ProductExposure_PrioritizationCoefficients':'BranchHier.DivisionName',
                'SalesPotentialsByTypeOfRecommendation':'BranchHier.DivisionName',
                'ProductExposure_PrioritizationCoefficients.dns_prv.dv':'load_offline_coeff',
                'SalesForecastByTypeRecommendations':'main.TypeRecommendationsID'}
                
                
                
def idtoname(df):
    """
    автоматом переводит все ID и Code в названия.
    в основном применяется для отправки файлов заказчикам
    """
    for col in df.columns.to_list():
        if col in ['BranchID','BranchCode']:
            col_dict=df_build('mart_com','branches',show_time=False)
            col_dict=col_dict[[col,'BranchName']]
            df=df.merge(col_dict,how='left',on=col)
            df=df.drop(columns=col)
        elif col in ['CategoryID','CategoryCode']:
            col_dict=df_build('mart_com','categories',show_time=False)
            col_dict=col_dict[[col,'CategoryName']]
            df=df.merge(col_dict,how='left',on=col)
            df=df.drop(columns=col)
        elif col in ['StoreID','BranchCode']:
            col_dict=df_build('mart_com','branches',show_time=False)
            col_dict=col_dict.groupby(col)[[col,'StoreName']].head(1)
            df=df.merge(col_dict,how='left',on=col)
            df=df.drop(columns=col)
    return df
    
    
hz="""  неудачная попытка сессий в КликХаусе. может ещё вернусь          
    elif dsn_database=='dns_log' and multi==True:
        from sqlalchemy import create_engine
        from sqlalchemy.orm import Session,sessionmaker
        
        #dsn_hostname = "adm-dv-ch.dns-shop.ru"
        #dsn_port = "8123" #
        #dsn_uid = "dns_Khramenkov"
        #dsn_pwd = "7EUxXo3NfLyb"
        #+native
        engine = create_engine(f'clickhouse://{dsn_uid}:{dsn_pwd}@{dsn_hostname}:{dsn_port}/{dsn_database}')
        Session = sessionmaker(engine)
        with Session() as session:
            #conn = engine.connect()
            scripts=query.split(';')
            for scr in scripts[:-1]:
                print(scr)
                session.execute(scr)
                session.commit()
            result=session.execute(scripts[-1])
            table=pd.DataFrame(result.fetchall(),columns=result.keys())
            #conn.close()
"""



def split_merge(df1,df2,how,on,col,partion_value=8,reset=True):
    """
    merge на случай больших датафреймов
    делим на порции и мёрджим по ним, потом соединяем
    на вход:
        df1 (df) - первый датафрейм
        df2 (df) - второй датафрейм
        how (str) - how из обычного merge
        on (str) - on из обычного merge
        col (str) - колонка, по которой разделяем датафреймы на порции
        partion_value (int) - сколько уникальных значений берём в одну порцию
        reset (bool) - надо ли перегружать индексы (если потом будем фильтровать по индексам - обязательно)
    на выход:
        мёрд двух переданных датафреймов
    """
    
    # на случаи, когда merge отъедает много памяти
    
    if 'result_split_merge' in dir():
        del result_split_merge
    
    if how in ['left','inner']:
        values=df1[col].drop_duplicates()
    else:
        values=pd.concat([df1[col],df2[col]]).drop_duplicates()
    
    
    values=values.to_list()
    values=[ values[x:x+partion_value] for x in range(0,len(values),partion_value)]
    
    for i in values:
        df10=df1[df1[col].isin(i)]
        try:
            df20=df2[df2[col].isin(i)]
        except:
            df20=df2.copy()
        temp=df10.merge(df20,how=how,on=on)
        
        if 'result_split_merge' not in dir():
            result_split_merge=temp.copy()
        else:
            #result_split_merge=result_split_merge.append(temp)
            result_split_merge=pd.concat([result_split_merge,temp],axis=0)
            
        df1=df1[~df1[col].isin(i)]
        
        try:
            df2=df2[~df2[col].isin(i)]
        except:
            pass
    #toSkype('split merge закончили','Airflow')    
        #result.append(temp)
    
    if 'result_split_merge' not in dir():
        result_split_merge=pd.DataFrame()
    
    if reset:
        return result_split_merge.reset_index(drop=True)#pd.concat([i for i in result])#.reset_index(drop=True)
    else:
        return result_split_merge
        
        
def update_branches():
    branches=df_build('dns_dwh','branches_dnsdwh')
    df_build('delta_bi','select 1 cnt','delete from public.branches')
    df_load('delta_bi','public.branches',branches)
    
def update_products():
    products=df_build('dns_dwh','products_dnsdwh')
    df_build('delta_bi','select 1 cnt','delete from public.products')
    df_load('delta_bi','public.products',products)
    
def update_cats():
    cats=df_build('dns_dwh','categories_dnsdwh')
    df_build('delta_bi','select 1 cnt','delete from public.cats')
    df_load('delta_bi','public.cats',cats)
    
def update_cal():
    cal=df_build('dns_dwh','common_calendar')
    df_build('delta_bi','select 1 cnt','delete from public.cal')
    toSkype(str(cal.shape),'Airflow')
    df_load('delta_bi','public.cal',cal)
    
    
scr_filials_conc=""" -- филиалы-концентраторы
        select cast(b.code as varchar) BranchCode
        from branch_DIM_Branch b 
        left join "staging_РегистрСведений.ФилиалыФорматаНаполненияПоДвижениюТовара" st on b.SourceID =st.Фирма 
        where 1=1
        --and DateCloseID is null
        -- проверено на код=7 и коде Борсина
        and (CONVERT(VARCHAR(1000), "ФорматНаполненияПоДвижениюТовара", 2)='01' or type = 'Дисконт центр' or type = 'РРЦ')
        """    
    
def create_branches():
    branches=df_build('dns_dwh','branches_dnsdwh')
        
    filials_conc=df_build('dns_dwh',scr_filials_conc)
    filials_conc['conc']=1
    branches=branches.merge(filials_conc,how='left')
    branches.conc=branches.conc.fillna(0)
    
    scr=""" -- модели конфигурации филиала
    with main as
    (
    select  main."Филиал" branch_guid, cast(main."Данные" as int) branch_model_fig,rank() OVER (PARTITION BY main."Филиал" ORDER BY main."Период" DESC) ra
        from "cdc.adm-1c-dns-m.dns_m"."RS.IstorijaFiliala" main
            left join "cdc.adm-1c-dns-m.dns_m"."RS.SsylkiPerechislenij" pere on main."ВидДанных" =pere."ЗначениеПеречисления" 
        where pere."Наименование" ='Модель конфигурации филиала'
        ),
        model as
        (
        select "ЗначениеХарактеристики_Число" branch_model_fig,"PredstavlenieZnachenijaHarakteristiki" branch_model_name
            from "cdc.adm-1c-dns-m.dns_m"."PVH.VidyHarakteristikPodrazdelenij.SpisokVybora"
            where "Ссылка"='bbda290a-acda-11ed-9099-00155d8ed20b'
            )
        select branch_guid,branch_model_name
        from main
        left join model on main.branch_model_fig=model.branch_model_fig
        where ra=1
    """    
    branch_model=df_build('cdc_current_state',scr)
    branches=branches.merge(branch_model,how='left')
    branches.branch_model_name=branches.branch_model_name.fillna('x')

    
    return branches
    
    
def create_cats():
    cats=df_build('dns_dwh','categories_dnsdwh')
        
    scr=""" -- общие категории
    select pr.categoryid "CategoryID",char_value.name CategoryCommonName,count(*) cnt
    from product_DIM_Product pr                          
        inner join product_DIM_Category c on pr.categoryid=c.ID 
        inner join product_FACT_1cCharacteristic char_fact on pr.id=char_fact.ProductID
        left join product_DIM_1cCharacteristicType char_type on char_fact.[1cCharacteristicTypeID]=char_type.id
        left join product_DIM_1cCharacteristic char_value on char_fact.[1cCharacteristicID]=char_value.id
        --left join product_DIM_Category c2 on char_value.name=c2.name 
        --left join product_DIM_Category c2 on char_value.SourceID=c2.SourceID
    where char_type.name = 'Категория общая'
        --and lower(c.Name) like '%смартфон%'
        --and lower(c.Name) <> 'Смартфон'
        and pr.isdeleted=0
        --and pr.categoryid=206
    group by pr.categoryid,char_value.name   
    """
    common=df_build('dns_dwh',scr)
    
    c=common.merge(cats['CategoryID'],on='CategoryID',how='inner')
    c['cnt_all']=c.groupby('CategoryID').cnt.transform(sum)
    c['cut']=c.cnt/c.cnt_all
    
    cats=cats.merge(c.query('cut>=0.95')[['CategoryID','CategoryCommonName']],on='CategoryID',how='left')
    
    cats.CategoryCommonName=cats.CategoryCommonName.fillna(cats.CategoryName)
    cats.CategoryCommonName=cats.CategoryCommonName.apply(lambda x: x.replace('"',''))
    
    if cats.CategoryID.count()!=cats.CategoryID.nunique(): # произошло задвоение
        raise Exception('Задвоение при определении общих категорий')
    return cats
    
    
def create_products_my():
    products=df_build('dns_dwh','products_dnsdwh')
    cats=df_build('dns_dwh','categories_dnsdwh')
    folders=df_build('delta_bi','select * from etl.products_good_nomenclature')
    
    products=products.merge(cats[['CategoryID','category1_number']],how='inner')\
            .merge(folders,how='inner')\
            .query('category1_number>=1 and category1_number<=9 and product_type in ("Товар","Агрегат")')
    products=products.drop(columns='category1_number')
    
    
    scr="""
    select pr.code ProductCode,char_value.name CategoryCommonName
    from product_DIM_Product pr                          
        --left join product_DIM_Category c on pr.categoryid=c.ID 
        left join product_FACT_1cCharacteristic char_fact on pr.id=char_fact.ProductID
        left join product_DIM_1cCharacteristicType char_type on char_fact.[1cCharacteristicTypeID]=char_type.id
        left join product_DIM_1cCharacteristic char_value on char_fact.[1cCharacteristicID]=char_value.id
        --left join product_DIM_Category c2 on char_value.name=c2.name 
        --left join product_DIM_Category c2 on char_value.SourceID=c2.SourceID
    where char_type.name = 'Категория общая'
        --and lower(c.Name) like '%смартфон%'
        --and lower(c.Name) <> 'Смартфон'
        and pr.isdeleted=0    
    """
    common_category=df_build('dns_dwh',scr)
    common_category=common_category.merge(cats[['CategoryName','category_guid']],left_on='CategoryCommonName',right_on='CategoryName',how='inner')
    common_category=common_category.drop(columns=['CategoryCommonName','CategoryName']).rename(columns={'category_guid':'category_common_guid'})
    
    products=products.merge(common_category,on='ProductCode',how='left')
    
    return products
    
    
def update_layers_hand_guids():
    """
    только по РФ
    обновляет справочник фиксаций
    самое главное - подгружает признаки фиксаций-слоёв и фиксаций-ручных
    """
    
    scr_load="""
        select cal1."Код" code_1c,cal1."Наименование" name,
        lower(
            CONCAT(
                SUBSTRING(CONVERT(VARCHAR(32), cal1."Ссылка", 2), 25, 8),
                '-', SUBSTRING(CONVERT(VARCHAR(32), cal1."Ссылка", 2), 21, 4),
                '-', SUBSTRING(CONVERT(VARCHAR(32), cal1."Ссылка", 2), 17, 4),
                '-', SUBSTRING(CONVERT(VARCHAR(32), cal1."Ссылка", 2), 1, 4),
                '-', SUBSTRING(CONVERT(VARCHAR(32), cal1."Ссылка", 2), 5, 12)
            )
        ) guid
        FROM dwh.[Справочник.АвтораспределениеВидыРекомендаций] cal1
        left join dwh.[Справочник.АвтораспределениеВидыРекомендаций] cal2 on cal1.Родитель =cal2.Ссылка 
        WHERE cal2."Наименование" = '@value'
        """
        
    scr_load="""
        select cal1."Код" code_1c,cal1."Наименование" name,
            lower(
                CONCAT(
                    SUBSTRING(CONVERT(VARCHAR(32), cal1."Ссылка", 2), 25, 8),
                    '-', SUBSTRING(CONVERT(VARCHAR(32), cal1."Ссылка", 2), 21, 4),
                    '-', SUBSTRING(CONVERT(VARCHAR(32), cal1."Ссылка", 2), 17, 4),
                    '-', SUBSTRING(CONVERT(VARCHAR(32), cal1."Ссылка", 2), 1, 4),
                    '-', SUBSTRING(CONVERT(VARCHAR(32), cal1."Ссылка", 2), 5, 12)
                )
            ) guid
            FROM dwh.[Справочник.АвтораспределениеВидыРекомендаций] cal1
            left join dwh.[Справочник.АвтораспределениеВидыРекомендаций] cal2 on cal1.Родитель =cal2.Ссылка 
            WHERE 1=1
                and lower(
                CONCAT(
                    SUBSTRING(CONVERT(VARCHAR(32), cal1."Группа", 2), 25, 8),
                    '-', SUBSTRING(CONVERT(VARCHAR(32), cal1."Группа", 2), 21, 4),
                    '-', SUBSTRING(CONVERT(VARCHAR(32), cal1."Группа", 2), 17, 4),
                    '-', SUBSTRING(CONVERT(VARCHAR(32), cal1."Группа", 2), 1, 4),
                    '-', SUBSTRING(CONVERT(VARCHAR(32), cal1."Группа", 2), 5, 12)
                )
            )='@value'
        """
        
    scr_insert="""
        update delta_bi.etl_calculus
        set layer_or_hand='@value'
        where guid in (@guids)
        """
    calculus=df_build('dep_spb',"""select *,1 in_calculus from delta_bi.etl_calculus where country='ru' """)
    
    for typ in [['layer','210acafb-e3e4-411d-a713-f82ea99c7553'],['hand','d1b09919-c021-43bd-b246-e19c8c6fcc96']]:
        try:
            replica=df_build('dns_m',scr_load,params={'@value':typ[1]})
            replica['country']='ru'
        except:
            toSkype('Синхронизация слоёв/ручных РФ\n\nреплика недоступна')
            return '' # предохранитель

        if replica.empty:
            toSkype('Синхронизация слоёв/ручных РФ - пустой датафрейм','Airflow')
            return '' # предохранитель
            
        # добавили новые
        replica=replica.merge(calculus[['guid','in_calculus']],how='left')
        to_load=replica[replica.in_calculus.isna()]
        if not to_load.empty:
            df_load('dep_spb','delta_bi.etl_calculus',to_load)
        
        # 
        #replica_good=replica.merge(calculus.query(f'layer_or_hand=="{typ[0]}"')[['guid','in_calculus']],how='left')
        
        replica_bad=replica.merge(calculus.query(f'layer_or_hand=="{typ[0]}"')[['guid','in_calculus']],how='right') # есть в моём справочнике с таким типом, но нет в реплике
        replica_bad=replica_bad[replica_bad.name.isna()]
        
        df_build('dep_spb','select 1 cnt',scr_insert,params={'@guids':str(replica_bad.guid.to_list())[1:-1],'@value':'x'})
        df_build('dep_spb','select 1 cnt',scr_insert,params={'@guids':str(replica.guid.to_list())[1:-1],'@value':typ[0]})
    toSkype('Синхронизация слоёв/ручных РФ \n\nпроведена')
    
    
    
def is_working():
    """
    проверяет рабочий день или нет
    на вход:
    
    на выход:
        return (bool)
    """
    dt=datetime.datetime.now(pytz.timezone('Asia/Irkutsk')).strftime('%Y-%m-%d')
    scr="""select DayType
        from dbo_DIM_Date dt
        where dt.date='@dt' and hour=0
        """
    test=df_build('dns_dwh',scr,params={'@dt':dt})
    return 'Рабочий' in test.DayType.item() # чтобы учесть "Рабочий сокр."
    
    
    
def upload_slice():
    scr="""
    select distinct toString(ProductCategory) category_guid,1 all_cats
    from dns_log.SalesForecastByTypeRecommendations main
            left join `default`.dict_products p on main.ProductID=p.Product 
    """
    cats=df_build('dns_log',scr)

    scr="""
    select toString(ProductCategory) category_guid,count(*),min(Period) min_period
    from dns_log.SalesForecastByTypeRecommendationsSlice main
            left join `default`.dict_products p on main.ProductID=p.Product 
    group by category_guid
    having min_period<'2023-10-23'
    """
    good_cats=df_build('dns_log',scr)

    cats=cats.merge(good_cats,how='left')
    cats=cats[cats.min_period.isna()]

    for category_guid in cats.category_guid.sort_values():

            scr=f"""
            insert into dns_log.SalesForecastByTypeRecommendationsSlice
            select main.*
            from dns_log.SalesForecastByTypeRecommendations main
                    inner join (
                            select Product
                            from `default`.dict_products
                            where ProductCategory='{category_guid}') p on main.ProductID=p.Product 
            order by Period desc
            limit 1 by TypeRecommendationsID,BranchID, ProductID
            """
            df_build('dns_log','select 1 cnt',scr)

    return ''
    
    
def create_donors():
    cats=df_build('dns_dwh','categories_dnsdwh')
    branches=df_build('dns_dwh','branches_dnsdwh')
    scr="""
    with main as
    (
    select top 1 with ties BranchID,SalesForecastBranchID ForecastBranchID,CategoryID
    -- select *
    --from product_FACT_DistributionSettings main
    from product_FACT_DistributionSettings main
    --where DistributionBranchID=6194
    where 1=1
        --and SalesForecastBranchID is not null
    order by row_number() over(partition by BranchID, CategoryID order by DateChange desc)
    )
    select *
    from main
    where ForecastBranchID is not null
    """
    forecast_branches0=df_build('dns_dwh',scr)
    
    # если есть связка филиал-категория - берём её. иначе - по пустой категории
    t1=forecast_branches0.merge(cats.CategoryID,how='inner')
    t2=forecast_branches0[forecast_branches0.CategoryID.isna()].drop(columns='CategoryID')
    forecast_branches=pd.concat([t1,t2],axis=0)


    brans=forecast_branches[['BranchID']].drop_duplicates()
    forecast_branches=brans.merge(cats.CategoryID,how='cross') # все сочетания филиал/категория
    forecast_branches=forecast_branches.merge(t1.rename(columns={'ForecastBranchID':'ForecastBranchID_cat'}),how='left',on=['BranchID','CategoryID']) # подтягиваем конкретные категории
    forecast_branches=forecast_branches.merge(t2.rename(columns={'ForecastBranchID':'ForecastBranchID_null'}),how='left',on=['BranchID']) # нулевые категории
    forecast_branches['ForecastBranchID']=forecast_branches.ForecastBranchID_cat
    forecast_branches['ForecastBranchID']=forecast_branches['ForecastBranchID'].fillna(forecast_branches['ForecastBranchID_null'])

    forecast_branches=forecast_branches[~forecast_branches.ForecastBranchID.isna()]
    forecast_branches=forecast_branches.merge(branches[['BranchID','branch_guid','BranchCode']],how='inner')\
                .merge(branches[['BranchID','branch_guid']].rename(columns={'BranchID':'ForecastBranchID','branch_guid':'Forecastbranch_guid'}),how='inner')\
                .merge(cats[['CategoryID','category_guid']],how='inner')
    return forecast_branches[['branch_guid','Forecastbranch_guid','category_guid','BranchCode']]
    
    
    
def get_layers(country):
    maxdt=df_build('delta_bi',f"select coalesce(max(dt),'1970-01-01') dt from hash_tables.negative_recomm_layers where country='{country}'")

    hash_dt=maxdt.dt[0]
    hash_delta=(datetime.datetime.now(tz)-tz.localize(hash_dt)).total_seconds()//3600
    
    if hash_delta<=1:
        temp=df_build('delta_bi',f"select branch_guid, product_guid from hash_tables.negative_recomm_layers where country='{country}'") # в будущем выгружать полностью - с нулями и всеми видами фиксаций. отсекать по текущим лежакам, 
        return temp # т.е. слои свежее двух часов, обновлять не надо
    
    scr="""
     with main as ( 
    select BranchID branch_guid,ProductID product_guid,TypeRecommendationsID,argMax(Quantity,rec.Period) Quantity
    -- select *
    FROM dns_log.ProductFixesByTypeRecommendationsNew rec
    where 1=1
    and rec.TypeRecommendationsID in (@guids) 
    --and rec.Period >='2024-03-18'
    group by BranchID, ProductID,TypeRecommendationsID 
    )
     select branch_guid,product_guid,TypeRecommendationsID,Quantity
     from main
    """
    layers=layer_guids()
    svod=[]
    for guid in layers.guid:
        # если вида фиксации нет в delta_bi, то грузить без отсечки даты. но тогда нули перегружать каждый раз?
        temp=df_build('dns_log','negative_recomm_fast',params={'@guids':"'"+guid+"'"},country=country,show_time=False)
        #if temp.Quantity.sum()>0: # только для первичной загрузки
        #    df_load('delta_bi','hash_tables.negative_recomm_layers',temp)
        svod.append(temp)
    
    #return ''
    
    temp=pd.concat(svod).drop_duplicates()
    
    temp['country']=country
    temp['dt']=datetime.datetime.now(tz)
    
    
    scr_delete=f"""
    delete
    from hash_tables.negative_recomm_layers
    where country='{country}'
    """
    df_build('delta_bi','select 1 cnt',scr_delete)
    
    df_load('delta_bi','hash_tables.negative_recomm_layers',temp)
    
    return temp
    
    
    
MeasureID_sku='a96f1829-9f7e-11ec-85b0-0050569d29c7'
MeasureID_item='6d604ea2-bb9e-11da-ab47-0002b3552d75'

RECOMM_TYPE_full_federal='179478e6-cdd7-11ed-90bd-00155d8ed20b'
RECOMM_TYPE_vitrina_federal='26670a0f-cdd7-11ed-90bd-00155d8ed20b'

RECOMM_TYPE_full_reg='179478e7-cdd7-11ed-90bd-00155d8ed20b'
RECOMM_TYPE_vitrina_reg='2d5aafbc-cdd7-11ed-90bd-00155d8ed20b'


RECOMM_DICT={
            'main_fed':RECOMM_TYPE_full_federal,
            'main_reg':RECOMM_TYPE_full_reg,
            'main_vitrina_fed':RECOMM_TYPE_vitrina_federal,
            'main_vitrina_reg':RECOMM_TYPE_vitrina_reg
            }
            
            
MEASURE_DICT={
            'sku':MeasureID_sku,
            'item':MeasureID_item
            }
            
            
def create_calendar():
    scr="""
    select "ДатаКалендаря" as ds
                from "InformationRegistersManager"."РегламентированныйПроизво_5aa28"
                where "ДатаКалендаря">=now()-interval '4 year'
                and "ДатаКалендаря"<=now()+interval '4 year'
    """
    cal=pd.read_sql(scr,con=dep_spb)
    
        
    cal['year']=cal.ds.dt.year
    cal['month']=cal.ds.dt.month
    #cal['week']=0

    cal['yday']=cal.ds.apply(lambda x: x.timetuple().tm_yday)
    cal['mday']=cal.ds.apply(lambda x: x.timetuple().tm_mday)
    cal['wday']=cal.ds.apply(lambda x: x.isoweekday())

    week_begins=cal.query('wday==1')[['ds']]
    week_begins=week_begins.rename(columns={'ds':'week_begin'})
    temp=cal[['ds']].merge(week_begins,how='cross').query('week_begin<=ds').sort_values('week_begin',ascending=False).drop_duplicates(subset=['ds'],keep='first')
    cal=cal.merge(temp,on='ds')

    cal['year_mon']=cal.week_begin.dt.year
    cal['week_mon']=cal.ds.apply(lambda x: x.isocalendar().week)

    cal['month_begin']=cal.groupby(['year','month']).ds.transform(min)
    cal['maxmday']=cal.groupby(['year','month']).mday.transform(max)

    week_begins=cal.groupby(['year','week_begin'],as_index=False).ds.min()[['year','ds']]
    week_begins=week_begins.rename(columns={'ds':'week_begin2'})
    temp=cal[['year','ds']].merge(week_begins,how='left',on='year').query('week_begin2<=ds')
    temp=temp.groupby('ds',as_index=False).week_begin2.nunique()
    temp=temp.rename(columns={'week_begin2':'week'})
    cal=cal.merge(temp,on='ds')

    cal.week_begin=cal.week_begin.astype('datetime64[ns]')
    cal.ds=cal.ds.apply(lambda x:x.date())

    return cal