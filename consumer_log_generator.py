import MySQLdb
import csv
import requests
import logging
import argparse
import sys
import pika
import json



parser = argparse.ArgumentParser()
parser.add_argument("--iter", help="running iteration, write value like 1, 2, 3 or so on")
parser.add_argument("--queue", help="queue name to be logged to file")
parser.add_argument("--log_file_ext", help="file extention for this queue")
args = parser.parse_args()

logging.basicConfig(
    filename='results/iter_%s_log_generator_%s.log' % (args.iter, args.queue),
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
channel = connection.channel()
channel.exchange_declare(exchange='deals', exchange_type='topic', durable=True)

'''
Save string to file
'''
def save_to_file(string, filename):
    file = open(filename,'a')
    file.write(string)
    file.close() 


def callback(ch, method, properties, body):
    string = body.decode()
    logger.info('Saving: %s' % (string))
    save_to_file(string, 'results/iter_%s_%s.%s' % (args.iter, args.queue, args.log_file_ext))


def main():
    channel.basic_consume(callback, queue=args.queue, no_ack=True)
    channel.start_consuming()

if __name__ == '__main__':
    main()