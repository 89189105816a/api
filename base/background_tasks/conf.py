from dotenv import load_dotenv
import os
load_dotenv()

DB_ADDR = os.environ.get("DB_ADDR")
DB_PORT = os.environ.get("DB_PORT")
DB_NAME = os.environ.get("DB_DATABASE")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASSWORD")
BROKER_ADDRESS = ""
BROKER_USER = ""
BROKER_PORT = ""
BROKER_ENGINE = ""

STOCKS_ORIGIN_TABLE = "stocks_wb"
