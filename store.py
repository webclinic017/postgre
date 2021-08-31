import os

# import sqlalchemy
import psycopg2
from dotenv import load_dotenv

load_dotenv()

connection = psycopg2.connect(os.environ["DATABASE_URL"])
cursor = connection.cursor()

SAVE_DATA = " "


def populate_database():
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SAVE_DATA)


first_row = cursor.fetchone()

print(first_row)

connection.close()
