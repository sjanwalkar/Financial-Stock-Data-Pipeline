#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 12:38:02 2021

@author: anjaliv
"""

from kafka import KafkaProducer
import simplejson as json
import pandas as pd
import yfinance as yf

# Fetch all data from yahoo finance
anthem = yf.Ticker("ANTM")
apple = yf.Ticker("AAPL")
google = yf.Ticker("GOOG")
humana = yf.Ticker("HUM")
medtronic = yf.Ticker("MDT")
pfizer = yf.Ticker("PFE")
tesla = yf.Ticker("TSLA")
unitedHealth = yf.Ticker("UNH")

# Extract 15 years history for all companies
anthem_data = anthem.history(period='15y')
apple_data = apple.history(period='15y')
google_data = google.history(period='15y')
humana_data = humana.history(period='15y')
medtronic_data = medtronic.history(period='15y')
pfizer_data = pfizer.history(period='15y')
tesla_data = tesla.history(period='15y')
unitedHealth_data = unitedHealth.history(period='15y')

# Create producer instance
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m, default=str).encode('ascii'))

# Send data to consumer
companyName="Anthem"
for index, row in anthem_data.iterrows():
    date=pd.to_datetime(index).date()
    producer.send('stockInformation',{'companyname': companyName, 'date': date,'adjclose': 0,'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': int(row['Volume'])})

companyName="Apple"
for index, row in apple_data.iterrows():
    date=pd.to_datetime(index).date()
    producer.send('stockInformation',{'companyname': companyName, 'date': date,'adjclose': 0,'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': int(row['Volume'])})

companyName="Google"
for index, row in google_data.iterrows():
    date=pd.to_datetime(index).date()
    producer.send('stockInformation',{'companyname': companyName, 'date': date,'adjclose': 0,'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': int(row['Volume'])})
    
companyName="Humana"
for index, row in humana_data.iterrows():
    date=pd.to_datetime(index).date()
    producer.send('stockInformation',{'companyname': companyName, 'date': date,'adjclose': 0,'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': int(row['Volume'])})
    
companyName="Medtronic"
for index, row in medtronic_data.iterrows():
    date=pd.to_datetime(index).date()
    producer.send('stockInformation',{'companyname': companyName, 'date': date,'adjclose': 0,'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': int(row['Volume'])})

companyName="Pfizer"
for index, row in pfizer_data.iterrows():
    date=pd.to_datetime(index).date()
    producer.send('stockInformation',{'companyname': companyName, 'date': date,'adjclose': 0,'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': int(row['Volume'])})
    
companyName="Tesla"
for index, row in tesla_data.iterrows():
    date=pd.to_datetime(index).date()
    producer.send('stockInformation',{'companyname': companyName, 'date': date,'adjclose': 0,'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': int(row['Volume'])})
    
companyName="UnitedHealth"
for index, row in unitedHealth_data.iterrows():
    date=pd.to_datetime(index).date()
    producer.send('stockInformation',{'companyname': companyName, 'date': date,'adjclose': 0,'close': row['Close'],'high': row['High'],'low': row['Low'], 'open': row['Open'],'volume': int(row['Volume'])})
    
# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=5)

