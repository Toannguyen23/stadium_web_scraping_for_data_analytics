import os, sys
from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.wikipedia_pipeline import *
dag = DAG(
    dag_id= "wikipedia_scraping_",
    default_args= {
        "owner": "Minh Toan",
        "start_date":days_ago(0)
    },
    schedule_interval=None,
    catchup=False
)

extract_data_from_wikipedia = PythonOperator(
    task_id= "extract_data_from_wikipedia",
    python_callable= extract_wikipedia_data,
    provide_context=True,
    op_kwargs={
        "url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"
    },
    dag=dag
)

transform_wikipedia_data = PythonOperator(
    task_id = "transform_wiki_pedia_data",
    provide_context=True,
    python_callable=transform_wikipedia_data,
    dag=dag
)

write_wikipedia_data = PythonOperator(
    task_id="write_wikipedia_data",
    provide_context=True,
    python_callable= write_wikipedia_data,
    dag=dag
)

extract_data_from_wikipedia >> transform_wikipedia_data >> write_wikipedia_data