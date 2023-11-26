import os
import random
import sqlalchemy
from sqlalchemy import text
from time import sleep
from dotenv import load_dotenv
import requests
import json
from datetime import datetime
import batch_ingestion as batch
import stream_ingestion as stream


class Core:
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

    def run_emulation_cycle(self, db_connector):
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        try:
            data = {
                "pin": db_connector.query_table('pinterest_data', random_row),
                "geo": db_connector.query_table('geolocation_data', random_row),
                "user": db_connector.query_table('user_data', random_row)
            }
            return data
        except Exception as e: print(f"Error occurred: {e}")

    def clock(self, output):
        while True:
            data = self.run_emulation_cycle(self)
            if data is not None:
                match output:
                    case "batch":
                        batch.BatchIngestor.to_msk(data["pin"], data["geo"], data["user"])
                    case "stream":
                        stream.StreamIngestor.to_kinesis(data["pin"], data["geo"], data["user"])
                    case "console":
                        self.to_console(data["pin"], data["geo"], data["user"])

    def to_console(self, pin_result, geo_result, user_result):
        print(f"PIN:\n{pin_result}\n")
        print(f"GEO:\n{geo_result}\n")
        print(f"USER:\n{user_result}\n")

if __name__ == "__main__":
    core_instance = Core()
    core_instance.clock("console")