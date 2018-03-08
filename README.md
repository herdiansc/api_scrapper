# Api Scrapper with Python and RabbitMQ

## How to Use
- Run publisher by executing command `python publisher.py --iter 1 --deal_csv_file deal_short.csv`
- Run consumer to scrap api by executing command `python consumer_api_scrapper.py --iter 1`. This consumer can be easily scaled by running multiple command,
- Run consumer to generate log by executing command `python consumer_log_generator.py --iter 1 --queue update_string --log_file_ext csv`
