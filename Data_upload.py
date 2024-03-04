import pandas as pd
from pandas import DataFrame
import requests, json, time, csv
from pathlib import Path
import numpy as np
import datetime

# Указываем данные по группе (точные данные указаны не будут)
token   = 'xxxxxxxxxxxx'
version = 2
id_rk   = 00000000

'''
Часть кода для выгрузки json формата данных
Если что-то пойдет не так (к примеру долгий ответ от сайта), то выгрузка будет прекращена и на выходе будут только скачанные данные
При выполнении запроса на выходе будут дополнительно отображаться кол-во скачанных данных (в одном offset - максимум 1000)
'''

offset_id = None
all_data = []
while True:
    req_stat = requests.post(
        url = 'https://senler.ru/api/subscribers/statSubscribe',
        timeout = 300,
        headers = {'Connection':'close'}, # после выполнения закрываем соединение
        params = {
        'vk_group_id'   : id_rk,
        'access_token'  : token,
        'v'             : version,
        'date_from'     : '01.01.2023 00:00:00',
        'date_to'       : '01.02.2023 00:00:00',
        'count'         : 1000,
        'offset_id'     : offset_id
    }
    )
    # print (req_stat.status_code)
    print (req_stat.reason)
    
    try:
        data        = req_stat.json()['items']
        offset_id   = req_stat.json().get('offset_id')
    except Exception as e:
        print ('SOMETHING IS WRONG')
        break

    print(offset_id, len(data))
    all_data.extend(data)
    
    if offset_id is None:              # выполнение останавливается, когда все данные за период выгружаются
        break
    

# создаем функцию для загрузки выкаченных json-данных в csv файл
def stat_writer (all_data):
    with open ('data.csv', 'w', encoding="utf-8") as file:
        a_pen = csv.writer(file)
        a_pen.writerow(('vk_user_id', 'action', 'date', 'source', 'utm_id'))
        for i in all_data:
                try:
                    a_pen.writerow(( 
                    i ['vk_user_id'],
                    i ['action'],
                    i ['date'],
                    i ['source'],
                    i ['utm_id']
                    ))
                except KeyError:
                    a_pen.writerow((
                    i ['vk_user_id'],
                    i ['action'],
                    i ['date'],
                    i ['source']
                    ))

stat_writer(all_data)                              # прогоняем функцию

Data = pd.read_csv(r'C:\Users\User_1\Desktop\project\VK_Sendler\2023\data.csv')            # Сохраняем данные в нужную папку