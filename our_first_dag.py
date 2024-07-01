import csv
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import logging
#from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



def postgres_to_s3():
    #from sqlalchemy import create_engine

    #engine = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/mynewdb')

    hook=PostgresHook(postgres_conn_id="postgres_localhost")
    conn=hook.get_conn()
    cursor=conn.cursor()
    cursor.execute("select * from test_data where currency='AUD'")

    with open ("dags/testdata1.txt","w") as f:
        csv_writer=csv.writer(f)
        csv_writer.writerow([i[0] for i in cursor.description])
        csv_writer.writerows(cursor)
    cursor.close()
    conn.close()

    logging.info("saved data in text file testdata1.txt")
    s3_hook=S3Hook(aws_conn_id="s3_conn")
    s3_hook.load_file(
        filename="dags/testdata1.txt",
        key="test_data/*",
        bucket_name="poc1-airflow-bucket"
    )


default_args={"owner":"airflow","start_date":datetime(2024,6,13)}

with DAG(dag_id="upload_to_s3",default_args=default_args,schedule_interval="@daily") as dag:

    task1=PythonOperator(
    task_id="post_to_s3",
    python_callable=postgres_to_s3
    
    )

