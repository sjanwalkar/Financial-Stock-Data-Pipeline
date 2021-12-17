#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka import kafkaUtils
from pyspark.sql import *
from pyspark.sql import SparkSession
import time
import json
#import org.apache.spark.sql.functions

if __name__=="__main__":
    '''
    spark= SparkSession.builder.master("local").appName("Kafka Spark Streaming").getOrCreate()
    sc = spark.SparkContext("yarn", "NetworkWordCount")
    ssc=StreamingContext(sc,10)
    
    '''
    kafka_bootstrap_servers = 'localhost:9092'
   # message=kafkaUtils.createDirectStream(ssc, topics=['project'],kafkaParams={"metadata.broker.list":"localhost:9092"})
    spark = SparkSession \
        .builder \
        .appName("Structured Streaming ") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    #data=message.map(lambda x:x[1])df = spark \
 
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
      .option("subscribe", "project") \
      .load()
    my_string = df.decode("utf-8")
    
    data = json.loads(my_string)
    print(data["Open"])
        
    '''
    def functordd(rdd):
        try:
            rdd1 = rdd.map(lambda x: jason.loads(x))
            df=spark.read.json(rdd1)
            df.show()
            #df.createOrReplaceTempView("Test")
            #df1=spark.sql("select Date, Open from Test")
            #df1.write.format('csv').mode('append').save("testing")
        except:
            pass
        
    data.foreachRDD(functordd)
    '''
    ssc.start()
    ssc.awaitTermination()

