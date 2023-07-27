FROM apache/airflow:2.6.3
RUN pip install mysql-connector-python
RUN pip install pymysql