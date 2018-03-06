import MySQLdb
import csv
import requests
import logging
import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("--iter", help="running iteration, write value like 1, 2, 3 or so on")
parser.add_argument("--deal_csv_file", help="csv file contains deal id and external id")
args = parser.parse_args()

logging.basicConfig(
    filename='results/iter_%s_process.log' % (args.iter),
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

# initiate mysql connection
connection = MySQLdb.connect('localhost', 'root', 'mysql', 'coba')
cursor = connection.cursor()



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
Query merchant_external table by name to check external merchant existence
'''
def check_merchant_external(external_merchant_name):
    logger.info('Checking merchant external %s existence', external_merchant_name)
    cursor.execute('select * from merchant_external where name = "%s"' % (external_merchant_name))
    return cursor.fetchone()

'''
Insert new external merchant to merchant_external table
'''
def insert_merchant_external(body):
    logger.info('Inserting %s', body['listing']['company']['name'])
    cursor.execute('INSERT INTO merchant_external VALUES(null, 3212, "%s", "%s", NOW());' % (body['listing']['company']['name'], body['listing']['company']['profile_icon_image']))
    connection.commit()
    return cursor.lastrowid

'''
Get fave deal by external_id
'''
def get_fave_deal(external_id):
    logger.info('Fetching deal %d', int(external_id))
    url = 'https://api.myfave.com/api/marketplace/v1/cities/jakarta/listings/%d?api_key=roPKaZMkCWqFKpfdbn5LmGwqmyoUW1KMyZcK' % (int(external_id))
    response = requests.get(url)

    body = response.json()

    if response.status_code == 200:
        if len(body['listing']['company']['name']) > 45:
            logger.info('[Process Failed] Merchant external name: %s for external id: %d is too long, ignored.' % (body['listing']['company']['name'], int(external_id)))
    else:
        logger.info('[Process Failed] Deal external id: %d, Status code: %d, Response: %s' % (int(external_id), response.status_code, body))

    return response

'''
Save string to file
'''
def save_to_file(string, filename):
    file = open(filename,'w')    
    file.write(string)
    file.close() 

'''
Main function
'''
def main():
    deal_update_string = ''
    not_insert_merchant_external_string = 'deal_id,deal_external_id,merchant_name,merchant_image,remark\n'
    logger.info('reading csv %s', args.deal_csv_file)
    logger.info('='*80)

    try:
        rows = read_csv(args.deal_csv_file)
        logger.info('found %d rows(deals)', len(rows))
        logger.info('='*80)
        i=1
        for row in rows:
            logger.info('Processing row %d/%d: deal id %d, deal external id %d.' % (i, len(rows), int(row['id']), int(row['external_id'])))            
            response = get_fave_deal(row['external_id'])

            body = response.json()

            if response.status_code == 200:
                if len(body['listing']['company']['name']) <= 45:
                    result = check_merchant_external(body['listing']['company']['name'])

                    if result is None:
                        logger.info('%s not exist', body['listing']['company']['name'])
                        external_merchant_id = insert_merchant_external(body)
                    else:
                        logger.info('%s exist', body['listing']['company']['name'])
                        external_merchant_id = result[0]

                    deal_update_string += '-- deal id: %d, external id: %d, merchant external name: %s \n' % (int(row['id']), int(row['external_id']), body['listing']['company']['name'])
                    deal_update_string += 'UPDATE deal SET external_merchant_id = %d WHERE id = %d;\n' % (int(external_merchant_id), int(row['id']))
                else:
                    not_insert_merchant_external_string += '"%d","%d","%s","%s","%s"\n' % (int(row['id']), int(row['external_id']), body['listing']['company']['name'], body['listing']['company']['profile_icon_image'], 'merchant_name too long')
            else:
                not_insert_merchant_external_string += '"%d","%d","%s","%s","%s"\n' % (int(row['id']), int(row['external_id']), '', '', body)

            logger.info('-'*80)
            i=i+1

        save_to_file(deal_update_string, 'results/iter_%s_update.sql' % (args.iter))
        save_to_file(not_insert_merchant_external_string, 'results/iter_%s_failed.csv' % (args.iter))
    
    except:
        logger.info('Unexpected error: %s', sys.exc_info()[0])
        raise

if __name__ == '__main__':
    main()
