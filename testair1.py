def main():
    import os

    # задаём рабочую папку
    #os.chdir(os.path.dirname(__file__)) 
    os.chdir('/home/mint/')
    try:
        import datetime
        with open('air_text.txt','a') as f:
            f.write(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') +';1\n')
    except Exception as e:
        print(e.__class__.__name__)
