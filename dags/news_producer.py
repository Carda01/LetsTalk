import json, datetime, os, tempfile, logging
from datetime import datetime
from newsapi import NewsApiClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from confluent_kafka import Producer

CATEGORIES = ['entertainment', 'sports', 'technology']

def fetch_from_news_api(**kwargs):
    try:
        conn = BaseHook.get_connection('news_api')
        api_key = conn.password
        logging.info(f"Initializing News API client, with key {api_key[:4]}...")
        newsapi = NewsApiClient(api_key)
        fetched_data_info = {}
        for category in CATEGORIES: 
            logging.info(f"Fetching from News API, category: {category}")
            top_headlines = newsapi.get_top_headlines(page_size=100,
                                                      category=category,
                                                      language='en')

            articles = top_headlines.get("articles")
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
                json.dump(articles, temp_file)
                fetched_data_info[category] = {}
                fetched_data_info[category]['path'] = temp_file.name
                fetched_data_info[category]['metadata'] = {"inserted_rows": top_headlines.get('totalResults')}
        return fetched_data_info
    except Exception as e:
        logging.error(f"Error while fetching: {str(e)}")
        raise

def publish_news_to_kafka(**kwargs):
    fetched_data_info = kwargs['ti'].xcom_pull(task_ids='fetch_from_news_api')
    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = Producer(conf)
    topic = 'news_topic'
    for category, fetched_info in fetched_data_info.items():
        with open(fetched_info['path'], 'r') as f:
            news_data = f.read()
        producer.produce(topic, key=category, value=news_data)
        producer.flush()
        os.unlink(fetched_info['path'])
        logging.info(f"Published {category} news to Kafka topic {topic}")

dag = DAG(
    dag_id='news_producer_dag',
    start_date=datetime(2025, 3, 1),
    description='A DAG that fetches news from the NewsAPI and publishes them to Kafka',
    schedule_interval="@hourly",
    catchup=False
)

fetch_news_task = PythonOperator(
    task_id='fetch_from_news_api',
    python_callable=fetch_from_news_api,
    provide_context=True,
    dag=dag
)

publish_news_task = PythonOperator(
    task_id='publish_news_to_kafka',
    python_callable=publish_news_to_kafka,
    provide_context=True,
    dag=dag
)

fetch_news_task >> publish_news_task
