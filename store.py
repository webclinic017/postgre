import os

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String, Date, DateTime, Float
from dotenv import load_dotenv
import EOD_api as api

load_dotenv()
TOKEN = os.environ["EOD_TOKEN"]
START = "2021-04-01"
END = "2021-04-02"
EXCHANGE_LIST = ["US"]
N_STOCKS_PER_EXCHANGE = 3

# Get the first n tickers by market cap for each exchange specified
stocks = list([])
for exchange in EXCHANGE_LIST:
    stocks.extend(
        api.stock_screener(
            n_stocks=N_STOCKS_PER_EXCHANGE,
            initial_offset=0,
            token=TOKEN,
            exchange=exchange,
            maxcap=None,
            mincap=None,
        )["code"]
    )
# Get the data
df = api.OhlcvIntraday(stocks, TOKEN, START, END, intraday_frec="5m").retrieve_data()
print(df.head(2), "\n")
# Initialize the connection to SQL Server
engine = create_engine(
    f'postgresql://root:{os.environ["PG_SERVER_PASSWORD"]}@localhost/test_db'
)
metadata = MetaData()
# Define the table
table = Table(
    "stock-data",
    metadata,
    Column("Stock", String(), index=True, nullable=False),
    Column("Date", DateTime(), index=True, nullable=False),
    Column("Timestamp", Integer(), nullable=False),
    Column("Gmtoffset", Float(), default=0),
    Column("Open", Float()),
    Column("High", Float()),
    Column("Low", Float()),
    Column("Close", Float()),
    Column("Volume", Float()),
)
# Create the table
metadata.drop_all(engine)
metadata.create_all(engine)
# Insert the data in the table
with engine.connect() as cnct:
    df.to_sql("stock-data", con=engine, if_exists="append")
    # Check the first 5 rows
    select_query = table.select()
    result = cnct.execute(select_query)
    for row in result.fetchmany(5):
        print(row)
