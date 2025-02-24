import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.reddit_pipeline import reddit_pipeline

default_args = {
    'owner': 'Fulito R',
    'start_date': datetime(2025, 01, 18)
}

file_postfix = datetime.now().strftime("%Y%m%d%H%M%S")

dag = DAG(
    'reddit_dag',
    default_args=default_args,
    description=' R Data Pipeline',
    schedule_interval='@daily',
    catchup=False,
    start_date=datetime(2025, 1, 18),
    tags=['reddit', 'etl', 'pipeline']
)

# extraction from reddit
extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'dataengineering',
        'time_filter': 'day',
        'limit': 100
    },
    dag=dag
)

extract