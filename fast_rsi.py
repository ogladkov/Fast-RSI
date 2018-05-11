import pandas as pd
import numpy as np
import bitmex
import smtrad
import time
import datetime as dt
import os

# Чтение файла с акками
login = input('Введите логин: ')
setdata = 'setdata.txt'
setdata = pd.read_csv(os.path.join(os.path.dirname(__file__),setdata), index_col = 'login')
api_key = setdata.loc[login, 'ak']
api_secret = setdata.loc[login, 'as']

# Подключение к BITMEX sm00th
client = bitmex.bitmex(test=True,
                       api_key = api_key,
                       api_secret = api_secret)

# Вводные данные
curr = setdata.loc[login, 'curr']
timeframe = setdata.loc[login, 'timeframe']
rsi_period=7
last_qt = client.Trade.Trade_getBucketed(symbol='XBTUSD', binSize='1h', count=1 , reverse=True).result()[0][0]['close']
balance = client.User.User_getWallet().result()[0]['amount'] * last_qt / 100000000
koeff = 4
orderQty = balance // koeff
poza = 0
time_dict = {'1m':60, '5m':300, '1h':3600}

print(login, timeframe, curr, orderQty)

# Основные функции

# Покупка
def go_long(amnt):
    buy = client.Order.Order_new(symbol=curr, orderQty=amnt, ordType='Market').result()
    global poza
    poza += buy[0]['orderQty']
    print('Купил: ', buy[0]['orderQty'], ' по курсу ', buy[0]['price'])
    print('Актуальная позиция: ', poza)
    
    poza_real = client.Position.Position_get().result()[0][0]['currentQty']
    discrepancy = poza - poza_real
    if discrepancy != 0:
        print('Расхождение: ', discrepancy)
        deal = client.Order.Order_new(symbol=curr, orderQty=discrepancy, ordType='Market').result()
           
# Продажа
def go_short(amnt):
    sell = client.Order.Order_new(symbol=curr, orderQty=amnt, ordType='Market').result()
    print(sell)
    global poza
    poza -= sell[0]['orderQty']
    print('Продал: ', sell[0]['orderQty'], ' по курсу', sell[0]['price'])
    print('Актуальная позиция: ', poza)
    
    poza_real = client.Position.Position_get().result()[0][0]['currentQty']
    discrepancy = poza - poza_real
    if discrepancy != 0:
        print('Расхождение: ', discrepancy)
        deal = client.Order.Order_new(symbol=curr, orderQty=discrepancy, ordType='Market').result()
        
# Применение индикаторов к датафрейму
def process():
    df = smtrad.read_bitmex(True, api_key, api_secret, curr, timeframe)
    df['BODYS'] = abs(df['CLOSE']-df['OPEN'])
    df['BODYS_MEAN'] = df['BODYS'].rolling(10).mean()
    df['COLOR'] = df.apply(lambda x: 1 if x['CLOSE'] - x['OPEN'] > 0 else 0, axis=1)
    df = smtrad.Indicator.rsi(df, rsi_period=rsi_period)
    return df

print(process().tail())

while True:
    if int(dt.datetime.strftime(dt.datetime.now(), format='%M')) < 1    :
        df = process()
        print(df.iloc[-2:][['CLOSE', 'BODYS', 'COLOR', 'RSI' + str(rsi_period)]], 
              '\n')

        buy = ''
        if (df.iloc[-1]['BODYS'] > df.iloc[-1]['BODYS_MEAN']/2 and 
           df.iloc[-1]['RSI' + str(rsi_period)] < 25 and 
           df.iloc[-1]['CLOSE'] < df.iloc[-2]['CLOSE'] and 
           df.iloc[-1]['COLOR'] == 0 and 
           df.iloc[-2]['COLOR'] == 0):
            print('Сигнал на покупку получен\n')
            if poza >= 0 and poza < koeff * orderQty:
                buy = go_long(orderQty)
            else:
                buy = go_long(-poza + orderQty)
                
                
        sell = ''
        if (df.iloc[-1]['BODYS'] > df.iloc[-1]['BODYS_MEAN']/2 and 
            df.iloc[-1]['RSI' + str(rsi_period)] > 75 and 
            df.iloc[-1]['CLOSE'] > df.iloc[-2]['CLOSE'] and 
            df.iloc[-1]['COLOR'] == 1 and 
            df.iloc[-2]['COLOR'] == 1):
            print('Сигнал на продажу получен\n')
            if poza <= 0 and poza > -koeff * orderQty:
                sell = go_short(-orderQty)
            else:
                sell = go_short(-poza - orderQty)
                
    time.sleep(60)