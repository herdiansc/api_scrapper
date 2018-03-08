import csv
import argparse
import sys
import logging
import pika
import json
import configparser

parser = argparse.ArgumentParser()
parser.add_argument("--iter", help="running iteration, write value like 1, 2, 3 or so on")
parser.add_argument("--deal_csv_file", help="csv file contains deal id and external id")
args = parser.parse_args()


logging.basicConfig(
    filename='results/iter_%s_publisher.log' % (args.iter),
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

rabbitMQconnection = pika.BlockingConnection(
    pika.URLParameters('amqp://%s:%s@%s:%s/' % (config['rabbitmq']['username'], config['rabbitmq']['password'], config['rabbitmq']['host'], config['rabbitmq']['port']))
)
channel = rabbitMQconnection.channel()
channel.exchange_declare(exchange='deals', exchange_type='topic', durable=True)
routing_key = 'deals.content'


'''
Read CSV
'''
def read_csv(filename):
    list_dict = []
    with open(filename, newline='') as csvfile:
        rows = csv.DictReader(csvfile)
        for row in rows:
            list_dict.append(row)
    return list_dict

'''
Main function
'''
def main():
    rows = read_csv(args.deal_csv_file)
    logger.info('Found: %d rows', len(rows))
    i=1
    for row in rows:
        logger.info('Inserting row: %d, deal: id %d, external id %d', i, int(row['id']), int(row['external_id']))
        channel.basic_publish(
            exchange='deals',
            routing_key=routing_key, 
            body=json.dumps(row),
            properties=pika.BasicProperties(content_type='application/json', delivery_mode=2)
        )
        i=i+1

if __name__ == '__main__':
    main()
