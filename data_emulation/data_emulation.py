import os
import random
import sqlalchemy
from sqlalchemy import text
from time import sleep
from dotenv import load_dotenv

class Core:
    def __init__(self):
        load_dotenv()
        self.host = os.getenv('RDS_HOST')
        self.user = os.getenv('RDS_USER')
        self.password = os.getenv('RDS_PASSWORD')
        self.database = 'pinterest_data'
        self.port = 3306
        self.engine = self.create_db_connector()

    def create_db_connector(self) -> sqlalchemy.engine.Engine:
        connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?charset=utf8mb4"
        return sqlalchemy.create_engine(connection_string)

    def query_table(self, table_name: str, row_number: int) -> dict:
        query_string = text(f"SELECT * FROM {table_name} LIMIT {row_number}, 1")
        with self.engine.connect() as connection:
            result = connection.execute(query_string)
            return dict(next(result)._mapping)

    def run_emulation_cycle(self) -> dict:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        try:
            return {
                "pin": self.query_table('pinterest_data', random_row),
                "geo": self.query_table('geolocation_data', random_row),
                "user": self.query_table('user_data', random_row)
            }
        except Exception as e:
            print(f"Error occurred: {e}")
            return {}

    def to_console(self, pin_result: dict, geo_result: dict, user_result: dict) -> None:
        print(f"PIN:\n{pin_result}\n")
        print(f"GEO:\n{geo_result}\n")
        print(f"USER:\n{user_result}\n")
