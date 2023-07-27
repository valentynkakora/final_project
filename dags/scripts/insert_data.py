import mysql.connector
import os
from scripts.coin_market_cap import CoinMarketCap
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()

CMC_API_KEY = os.environ.get('CMC_API_KEY')
user = os.environ.get('db_user')
password = os.environ.get('password')

mysql_connection = mysql.connector.connect(
    host="host.docker.internal",
    port=3306,
    user=user,
    password=password,
    database="crypto_final_project"
)


def insert_exchange_map():
    data = CoinMarketCap.get_exchanges_map("https://pro-api.coinmarketcap.com/v1/exchange/map", CMC_API_KEY)
    date = datetime.today()
    time = datetime.now()
    sql = "INSERT INTO exchange_map VALUES (%s, %s, %s, %s, %s, %s)"
    with mysql_connection as mydb:
        mycursor = mydb.cursor()
        for element in data:
            id = element["id"]
            slug = element["slug"]
            name = element["name"]
            first_historical_date = datetime.strptime(element["first_historical_data"], "%Y-%m-%dT%H:%M:%S.%fZ")
            values = (id, slug, name, first_historical_date, date, time)
            mycursor.execute(sql, values)
        mydb.commit()


def insert_exchanges_data():
    map_data = CoinMarketCap.get_exchanges_map("https://pro-api.coinmarketcap.com/v1/exchange/map", CMC_API_KEY)
    ids = list()
    for item in map_data:
        item_id = item["id"]
        ids.append(item_id)

    exchange_data = CoinMarketCap.get_exchanges_data(ids, CMC_API_KEY)
    date = datetime.today()
    time = datetime.now()
    sql = "INSERT INTO exchanges_data VALUES (%s, %s, %s, %s, %s)"
    with mysql_connection as mydb:
        mycursor = mydb.cursor()
        for element in exchange_data:
            id = element["id"]
            spot_volume_usd_24h = element["spot_volume_usd_24h"]
            weekly_visits = element["weekly_visits"]
            values = (id, spot_volume_usd_24h, weekly_visits, date, time)
            mycursor.execute(sql, values)
        mydb.commit()


def insert_unique_data():
    with mysql_connection as mydb:
        mycursor = mydb.cursor()
        mycursor.execute("truncate unique_exchange_names;")
        sql = """insert into unique_exchange_names
                 select distinct id, 
                 name 
                 from crypto_final_project.exchange_map"""
        mycursor.execute(sql)
        mydb.commit()
