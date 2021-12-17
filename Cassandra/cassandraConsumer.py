from cassandra.cluster import Cluster
from kafka import KafkaConsumer
from kafka.errors import KafkaError

import argparse
import atexit
import json
import logging

# - default kafka topic to read from
topic_name = 'stockInformation'

# - default kafka broker location
kafka_broker = '127.0.0.1:9092'

# - default cassandra nodes to connect
contact_points = '127.0.0.1'

# - default keyspace to use
key_space = 'bigdataproject'

# - default table to use
data_table = 'stocksinformation'

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage')
logger.setLevel(logging.DEBUG)


def persist_data(stock_data, cassandra_session):
    """
    persist stock data into cassandra
    :param stock_data:
        the stock data looks like this:
        [{
            "Index": "NASDAQ",
            "LastTradeWithCurrency": "109.36",
            "LastTradeDateTime": "2016-08-19T16:00:02Z",
            "LastTradePrice": "109.36",
            "LastTradeTime": "4:00PM EDT",
            "LastTradeDateTimeLong": "Aug 19, 4:00PM EDT",
            "StockSymbol": "AAPL",
            "ID": "22144"
        }]
    :param cassandra_session:
    :return: None
    """
    try:
        #logger.debug('Start to persist data to cassandra -------- %s -----------', stock_data)
        companyname = stock_data[6]['companyname']
        date = stock_data[6]['date']
        adjclose = stock_data[6]['adjclose']
        close = stock_data[6]['close']
        high = stock_data[6]['high']
        low = stock_data[6]['low']
        openval = stock_data[6]['open']
        volume = stock_data[6]['volume']
        
        #print("date = " + date)
        
        statement = "INSERT INTO %s (companyname, date, adjclose, close, high, low, open, volume) VALUES ('%s','%s', %s, %s, %s, %s, %s, %s)" % (data_table, companyname, date, adjclose, close, high, low, openval, volume)

        #print("insert statement = " + statement)
        cassandra_session.execute(statement)
        
        #logger.info('Persistend data to cassandra for (companyname, date, adjclose, close, high, low, open, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)' % (companyname, date, adjclose, close, high, low, openval, volume))
    except Exception as exception:        
        logger.error('Failed to persist data to cassandra %s due to %s', stock_data, exception)


def shutdown_hook(consumer, session):
    """
    a shutdown hook to be called before the shutdown
    :param consumer: instance of a kafka consumer
    :param session: instance of a cassandra session
    :return: None
    """
    try:
        logger.info('Closing Kafka Consumer')
        consumer.close()
        logger.info('Kafka Consumer closed')
        logger.info('Closing Cassandra Session')
        session.shutdown()
        logger.info('Cassandra Session closed')
    except KafkaError as kafka_error:
        logger.warn('Failed to close Kafka Consumer, caused by: %s', kafka_error.message)
    finally:
        logger.info('Existing program')


if __name__ == '__main__':
    # - setup command line arguments
    parser = argparse.ArgumentParser()
    
    # python cassandraConsumer.py stockInformation 127.0.0.1:9092 bigdataproject stocksinformation 127.0.0.1

    parser.add_argument('topic_name', help='the kafka topic to subscribe from')
    parser.add_argument('kafka_broker', help='the location of the kafka broker')
    parser.add_argument('key_space', help='the keyspace to use in cassandra')
    parser.add_argument('data_table', help='the data table to use')
    parser.add_argument('contact_points', help='the contact points for cassandra')

    # - parse arguments
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    key_space = args.key_space
    data_table = args.data_table
    contact_points = args.contact_points

    # - initiate a simple kafka consumer
    #consumer = KafkaConsumer(
    #    topic_name,
    #    bootstrap_servers=kafka_broker
    #)

    consumer = KafkaConsumer(topic_name,bootstrap_servers = ['localhost:9092'],value_deserializer=lambda m: json.loads(m.decode('ascii')))

    # - initiate a cassandra session
    cassandra_cluster = Cluster(
        contact_points=contact_points.split(',')
    )
    session = cassandra_cluster.connect()
    session.set_keyspace(key_space)

    # - setup proper shutdown hook
    atexit.register(shutdown_hook, consumer, session)
    
    # Read data from kafka
    for message in consumer:
        persist_data(message, session)
