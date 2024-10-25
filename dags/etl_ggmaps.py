from airflow import DAG
from ggmaps.crawl import crawl_tasks
from ggmaps.transform import processing_tasks
from dags.ggmaps.load import load_tasks
from datetime import datetime

with DAG(
        dag_id='ggmaps_pipeline',
        start_date=datetime(year=2024, 
                            month=9, 
                            day=28),
        schedule_interval='30 9 * * *',
        catchup=False) as dag:

    crawl = crawl_tasks()

    processing = processing_tasks()

    load = load_tasks()

    crawl >> processing >> load