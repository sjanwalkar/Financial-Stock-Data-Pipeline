#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 14:00:03 2021

@author: anjaliv
"""

from kafka import KafkaProducer
import simplejson as json
import pandas as pd

col_list = ["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('ascii'))

csv_data = pd.read_csv('./datasets/anthem.csv', usecols=col_list)
companyName="Anthem"
for index, row in csv_data.iterrows():
    producer.send('stockInformation',{'companyname': companyName, 'date': row['Date'],'adjclose': row['Adj Close'],'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': row['Volume'] })
    
csv_data = pd.read_csv('./datasets/apple.csv', usecols=col_list)
companyName="Apple"
for index, row in csv_data.iterrows():
    producer.send('stockInformation',{'companyname': companyName, 'date': row['Date'],'adjclose': row['Adj Close'],'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': row['Volume'] })

csv_data = pd.read_csv('./datasets/google.csv', usecols=col_list)
companyName="Google"
for index, row in csv_data.iterrows():
    producer.send('stockInformation',{'companyname': companyName, 'date': row['Date'],'adjclose': row['Adj Close'],'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': row['Volume'] })
    
csv_data = pd.read_csv('./datasets/humana.csv', usecols=col_list)
companyName="Humana"
for index, row in csv_data.iterrows():
    producer.send('stockInformation',{'companyname': companyName, 'date': row['Date'],'adjclose': row['Adj Close'],'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': row['Volume'] })
    
csv_data = pd.read_csv('./datasets/medtronic.csv', usecols=col_list)
companyName="Medtronic"
for index, row in csv_data.iterrows():
    producer.send('stockInformation',{'companyname': companyName, 'date': row['Date'],'adjclose': row['Adj Close'],'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': row['Volume'] })    

csv_data = pd.read_csv('./datasets/pfizer.csv', usecols=col_list)
companyName="Pfizer"
for index, row in csv_data.iterrows():
    producer.send('stockInformation',{'companyname': companyName, 'date': row['Date'],'adjclose': row['Adj Close'],'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': row['Volume'] })        
    
csv_data = pd.read_csv('./datasets/tesla.csv', usecols=col_list)
companyName="Tesla"
for index, row in csv_data.iterrows():
    producer.send('stockInformation',{'companyname': companyName, 'date': row['Date'],'adjclose': row['Adj Close'],'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': row['Volume'] })        
    
csv_data = pd.read_csv('./datasets/unitedHealth.csv', usecols=col_list)
companyName="UnitedHealth"
for index, row in csv_data.iterrows():
    producer.send('stockInformation',{'companyname': companyName, 'date': row['Date'],'adjclose': row['Adj Close'],'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': row['Volume'] })        

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)

