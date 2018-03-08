# Api Scrapper with Python and RabbitMQ

This is a console application to scrap an api implemented by python programming language and rabbitmq.

This app will read a csv file containing a bunch of two id pairs. An api scrapping will be performed for every row of id pair, after scrapping done than an insert query to mysql db will be performed and log will be created.

There are 3 components:

- Publisher
- Consumer: API Scrapper
- Consumer: Log Generator 

## How to Use
- Run publisher by executing command `python publisher.py --iter 1 --deal_csv_file deal_short.csv`
- Run consumer to scrap api by executing command `python consumer_api_scrapper.py --iter 1`. This consumer can be easily scaled by running multiple command,
- Run consumer to generate log by executing command `python consumer_log_generator.py --iter 1 --queue update_string --log_file_ext csv`
