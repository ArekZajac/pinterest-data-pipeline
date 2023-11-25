import os
import random
import sqlalchemy
from sqlalchemy import text
from time import sleep
from dotenv import load_dotenv
import requests
import json
from datetime import datetime


class AWSDBConnector:
    def __init__(self):
        load_dotenv()
        self.HOST = os.getenv('RDS_HOST')
        self.USER = os.getenv('RDS_USER')
        self.PASSWORD = os.getenv('RDS_PASSWORD')
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        self.engine = self.create_db_connector()

    def create_db_connector(self):
        connection_string = f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4"
        return sqlalchemy.create_engine(connection_string)

    def query_table(self, table_name, row_number):
        query_string = text(f"SELECT * FROM {table_name} LIMIT {row_number}, 1")
        with self.engine.connect() as connection:
            result = connection.execute(query_string)
            for row in result:
                return dict(row._mapping)

def run_emulation_clock(db_connector):
    while True:
        try:
            sleep(random.randrange(0, 2))
            random_row = random.randint(0, 11000)

            pin_result = db_connector.query_table('pinterest_data', random_row)
            geo_result = db_connector.query_table('geolocation_data', random_row)
            user_result = db_connector.query_table('user_data', random_row)

            # to_console(pin_result, geo_result, user_result)
            to_msk(pin_result, geo_result, user_result)

        except Exception as e:
            print(f"Error occurred: {e}")

def to_console(pin_result, geo_result, user_result):
    print(f"PIN:\n{pin_result}\n")
    print(f"GEO:\n{geo_result}\n")
    print(f"USER:\n{user_result}\n")
    

if __name__ == "__main__":
    new_connector = AWSDBConnector()
    run_infinite_post_data_loop(new_connector)