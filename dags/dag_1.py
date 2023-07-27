from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.insert_data import insert_exchanges_data, insert_exchange_map, insert_unique_data
from scripts.analysis import get_top_exchanges_trading_volume, get_top_exchanges_spot_volume


with DAG(
    dag_id="visits_and_trading_volume",
    start_date= datetime(2023, 7, 24, 14, 0),
    schedule_interval="0 12 * * *"
) as dag:
    exchange_map_task = PythonOperator(
        task_id="insert_exchange_map",
        python_callable=insert_exchange_map
    )
    exchange_data_task = PythonOperator(
        task_id="insert_exchange_data",
        python_callable=insert_exchanges_data
    )
    unique_exchanges_names = PythonOperator(
        task_id="get_unique_exchanges",
        python_callable=insert_unique_data
    )
    top_ten_by_weekly_visits = PythonOperator(
        task_id="top_ten_by_weekly_visits",
        python_callable=get_top_exchanges_trading_volume,
        op_kwargs={
            "top_n": 10,
            "filepath": "/opt/airflow/csv_data/top_10_weekly_visitors.csv"
        }
    )
    top_ten_by_spot_volume = PythonOperator(
        task_id="top_ten_by_spot_volume",
        python_callable= get_top_exchanges_spot_volume,
        op_kwargs= {
            "top_n": 10,
            "filepath": "/opt/airflow/csv_data/top_10_spot_volume.csv"
        }
    )

    exchange_map_task >> exchange_data_task >> top_ten_by_weekly_visits
    exchange_map_task >> unique_exchanges_names >> top_ten_by_weekly_visits
    exchange_map_task >> exchange_data_task >> top_ten_by_spot_volume
    exchange_map_task >> unique_exchanges_names >> top_ten_by_spot_volume
