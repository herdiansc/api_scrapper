import MySQLdb
import csv
import requests
import logging
import argparse
import sys
import pika
import json
import configparser



parser = argparse.ArgumentParser()
parser.add_argument("--iter", help="running iteration, write value like 1, 2, 3 or so on")
args = parser.parse_args()

logging.basicConfig(
    filename='results/iter_%s_consumer.log' % (args.iter),
    format='%(asctime)s %(levelname)-8s %(message)s', 
    level=logging.INFO
)

# print to console
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

logger = logging.getLogger()


config = configparser.ConfigParser()
config.read('config.ini')

# initiate mysql connection
connection = MySQLdb.connect(
    config['mysql']['host'], 
    config['mysql']['username'], 
    config['mysql']['password'], 
    config['mysql']['database']
)
connection.autocommit(True)
cursor = connection.cursor()


rabbitMQconnection = pika.BlockingConnection(
    pika.URLParameters('amqp://%s:%s@%s:%s/' % (config['rabbitmq']['username'], config['rabbitmq']['password'], config['rabbitmq']['host'], config['rabbitmq']['port']))
)
channel = rabbitMQconnection.channel()
channel.exchange_declare(exchange='deals', exchange_type='topic', durable=True)
routing_key = 'deals.content'

'''
Query merchant_external table by name to check external merchant existence
'''
def check_data(external_merchant_name):
    logger.info('Checking data "%s" existence', external_merchant_name)
    cursor.execute('select * from merchant_external where name = "%s"' % (external_merchant_name))
    return cursor.fetchone()

'''
Insert new data to merchant_external table
'''
def insert_data(body):
    logger.info('Inserting data "%s"', body['listing']['company']['name'])
    cursor.execute('INSERT INTO merchant_external VALUES(null, 3212, "%s", "%s", NOW());' % (body['listing']['company']['name'], body['listing']['company']['profile_icon_image']))
    return cursor.lastrowid

'''
Get fave deal by external_id
'''
def fetch_api(external_id):
    logger.info('Fetching %d', int(external_id))
    url = config['api']['url'].format(external_id)
    response = requests.get(url)

    body = response.json()

    if response.status_code == 200:
        if len(body['listing']['company']['name']) > int(config['application']['company_name_max_length']):
            logger.info('[Process Failed] Merchant external name: %s for external id: %d is too long, ignored.' % (body['listing']['company']['name'], int(external_id)))
    else:
        logger.info('[Process Failed] Deal external id: %d, Status code: %d, Response: %s' % (int(external_id), response.status_code, body))

    return response

def publish(routing_key, message):
    channel.basic_publish(
        exchange='deals',
        routing_key=routing_key, 
        body=message,
    )


def callback(ch, method, properties, body):
    row = json.loads(body)
    logger.info('Processing id %d, external id %d.' % (int(row['id']), int(row['external_id'])))            
    response = fetch_api(row['external_id'])

    body = response.json()

    if response.status_code == 200:
        if len(body['listing']['company']['name']) <= int(config['application']['company_name_max_length']):
            result = check_data(body['listing']['company']['name'])
            logger.info('%s', result)
            
            try:
                if result is None:
                    logger.info('%s not exist', body['listing']['company']['name'])
                    external_merchant_id = insert_data(body)
                else:
                    logger.info('%s exist', body['listing']['company']['name'])
                    external_merchant_id = result[0]

                deal_update_string = '-- deal id: %d, external id: %d, merchant external name: %s \n' % (int(row['id']), int(row['external_id']), body['listing']['company']['name'])
                deal_update_string += 'UPDATE deal SET external_merchant_id = %d WHERE id = %d;\n' % (int(external_merchant_id), int(row['id']))
                publish('deals.update', deal_update_string)

                ch.basic_ack(method.delivery_tag)

            except:
                logger.info('Insert error: %s', sys.exc_info()[0])
                ch.basic_nack(method.delivery_tag)

        else:
            publish('deals.failed', '"%d","%d","%s","%s","%s"\n' % (int(row['id']), int(row['external_id']), body['listing']['company']['name'], body['listing']['company']['profile_icon_image'], 'merchant_name too long'))
            ch.basic_ack(method.delivery_tag)
    else:
        publish('deals.failed', '"%d","%d","%s","%s","%s"\n' % (int(row['id']), int(row['external_id']), '', '', body))
        ch.basic_ack(method.delivery_tag)

    logger.info('-'*80)


def main():
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue='csv_content')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()

if __name__ == '__main__':
    main()