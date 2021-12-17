#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from kafka import KafkaProducer
import time,csv

'''
input csv example

AT,BE,BG,CH,CY,CZ,DE,DK,EE,ES,FI,FR,EL,HR,HU,IE,IT,LT,LU,LV,NL,NO,PL,PT,RO,SI,SK,SE,UK
0.15104895104895097,0.155978142670726,0.0,0.132959173102667,0,0.0261248185776488,0.0314454263905056,0.0,0.0,0.22130378970001602,0.0,0.0881265984931488,0.09026049932169501,0.056874262941565,0.0841602727424313,0.0494006197388216,0.0912473405767843,0.0,0.0656217442366246,0.0,0.0432966804004962,0.0,0.0,0.19138755980861197,0.0,0.0521335743946527,0.0,0.0,0.0434660616908725

'''

# create producer to kafka connection
producer = KafkaProducer(bootstrap_servers='localhost:9092')
# define *.csv file and a char that divide value
fname = "C:/Users/sachi/Downloads/Hadoop/apple.csv"
divider_char = ','
# open file
with open(fname) as fp:  
    # read header (first line of the input file)
    line = fp.readline()
    header = line.split(divider_char)

    #loop other data rows 
    line = fp.readline()    
    while line:
        # start to prepare data row to send
        data_to_send = ""
        values = line.split(divider_char)
        len_header = len(header)
        for i in range(len_header):
            data_to_send += "\""+header[i].strip()+"\""+":"+"\""+values[i].strip()+"\""
            if i<len_header-1 :
                data_to_send += ","
        data_to_send = "{"+data_to_send+"}"

        '''
        example of outputs is valid JSON row 
        {
            "AT":"0.148251748251748",
            "BE":"0.052603706790461",
                ...
            "SE":"0.0826699344612236",
            "UK":"0.10951678628072099"
        }
        '''

        # send data via producer
        producer.send('project', bytes(data_to_send, encoding='utf-8'))
        line = fp.readline()
        # А это так))) на всякий случай
        #time.sleep(1)
producer.close()

