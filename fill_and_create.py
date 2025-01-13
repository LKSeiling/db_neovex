from decouple import Config, RepositoryEnv
import sys
import traceback
import csv
import psycopg2 as pg
from src.table_setup import *
from src.table_populate import fill_altnews, fill_legnews, fill_consp_label, fill_liwc_label, fill_4chan, fill_reddit, fill_twitter, fill_liwc_tweets, fill_tweets

csv.field_size_limit(sys.maxsize)


config = Config(RepositoryEnv('./.env'))
HOST = config.get('REMOTE_HOST')
UNAME = config.get('UNAME')
PW = config.get('PASSWORD')
DB_NAME = config.get('DB_NAME')
BASE_PATH = config.get('BASE_PATH')

# open connection and starting cursor to execute commands
connection = pg.connect(host=HOST, port=5432, dbname=DB_NAME, user=UNAME, password=PW)
cursor = connection.cursor()

# create tables in which to insert data
print("Setting up tables...")
# create_tables(cursor, connection)
print("Successfully set up tables!")


# # add data
# try:
#     print("(1) Populating AltNews table...")
#     fill_altnews(cursor, connection)
#     print("Successfully pupulated AltNews table!")
# except Exception as e:
#     print(f"Error during Population of AltNews table: {e}")

# try:
#     print("(2) Populating LegNews table...")
#     fill_legnews(cursor, connection)
#     print("Successfully pupulated AltNews table!")
# except Exception as e:
#     print(f"Error during Population of LegNews table: {e}")

# try:
#     print("(3.1) Populating tweet tables...")
#     fill_tweets(cursor, connection)
#     print("Successfully pupulated Twitter tables!")
# except Exception as e:
#     print(f"Error during Population of Twitter tables: {e}")

fill_reddit(cursor, connection)


# try:
#     print("(2) Populating Reddit table...")
#     fill_reddit(cursor, connection)
#     print("Successfully pupulated Reddit table!")
# except Exception as e:
#     traceback.print_exc(file=sys.stdout)
#     print(f"Error during Population of Reddit table: {e}")

# try:
#     print("(2) Populating 4chan table...")
#     fill_4chan(cursor, connection)
#     print("Successfully pupulated 4chan table!")
# except Exception as e:
#     print(f"Error during Population of 4chan table: {e}")


# try:
#     print("(3) Populating LIWC tweet tables...")
#     fill_liwc_tweets(cursor, connection)
#     print("Successfully pupulated Twitter tables!")
# except Exception as e:
#     print(f"Error during Population of Twitter tables: {e}")







connection.close()
cursor.close()
