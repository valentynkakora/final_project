import pandas as pd
import sqlalchemy
import os
from dotenv import load_dotenv
import csv

load_dotenv()

user = os.environ.get('db_user')
password = os.environ.get('password')
host = "host.docker.internal"
db_name = "crypto_final_project"
port = 3306

engine = sqlalchemy.create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db_name}")


def get_top_exchanges_trading_volume(top_n, filepath):
    with engine.connect() as connection:
        sql = f"""with weekly_vis_data as(
                    select 
                        uen.name,
                        ed.weekly_visits,
                        ed.date,
                        ed.time,
                        row_number() over(PARTITION BY ed.id ORDER BY ed.time) as row_num
                    from exchanges_data ed
                        inner join unique_exchange_names uen
                            on ed.id = uen.id),
                  ranking as(       
                    select 
                        name,
                        weekly_visits,
                        row_num,
                        wd.date,
                        wd.time,
                        dense_rank() over(order by weekly_visits desc) as rnk
                    from weekly_vis_data wd
                    where (name, row_num) in (
                        select name, max(row_num) as max_row_num
                        from weekly_vis_data
                        group by name) and weekly_visits is not NULL)
                    select
                        name,
                        weekly_visits,
                        date,
                        time
                    from ranking
                    where rnk <= {top_n};"""

        request = pd.read_sql(sql, con=connection)
        request["time"] = request["time"].astype(str).str[-8:-3]
        if os.path.isfile(filepath):
            with open(filepath, "a") as file:
                writer = csv.writer(file)
                for index, row in request.iterrows():
                    writer.writerow(
                        (
                            row[0],
                            row[1],
                            row[2],
                            row[3]
                        )
                    )
        else:
            request.to_csv(filepath, index=False)


def get_top_exchanges_spot_volume(top_n, filepath):
    with engine.connect() as connection:
        sql = f"""with weekly_vis_data as(
                    select 
                        uen.name,
                        ed.spot_volume_usd_24h,
                        ed.date,
                        ed.time,
                        row_number() over(PARTITION BY ed.id ORDER BY ed.time) as row_num
                    from exchanges_data ed
                        inner join unique_exchange_names uen
                            on ed.id = uen.id),
                  ranking as(       
                    select 
                        name,
                        spot_volume_usd_24h,
                        row_num,
                        wd.date,
                        wd.time,
                        dense_rank() over(order by spot_volume_usd_24h desc) as rnk
                    from weekly_vis_data wd
                    where (name, row_num) in (
                        select name, max(row_num) as max_row_num
                        from weekly_vis_data
                        group by name) and spot_volume_usd_24h is not NULL)
                    select
                        name,
                        spot_volume_usd_24h,
                        date,
                        time
                    from ranking
                    where rnk <= {top_n};"""

        request = pd.read_sql(sql, con=connection)
        request["time"] = request["time"].astype(str).str[-8:-3]
        if os.path.isfile(filepath):
            with open(filepath, "a") as file:
                writer = csv.writer(file)
                for index, row in request.iterrows():
                    writer.writerow(
                        (
                            row[0],
                            row[1],
                            row[2],
                            row[3]
                        )
                    )
        else:
            request.to_csv(filepath, index=False)
