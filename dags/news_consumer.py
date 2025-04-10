import json, os, logging, tempfile
from datetime import datetime
from confluent_kafka import Consumer
from lib.utils import get_spark_and_path, get_null_percentage
from airflow import DAG
from airflow.operators.python import PythonOperator
import time

def consume_news_from_kafka(**kwargs):
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'news_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(conf)
    topic = 'news_topic'
    consumer.subscribe([topic])
    messages = {}

    start_time = time.time()
    while time.time() - start_time < 30:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error(f"Consumer error: {msg.error()}")
            continue
        key = msg.key().decode('utf-8') if msg.key() else 'unknown'
        value = msg.value().decode('utf-8')
        # Assuming each message contains a JSON array of articles
        messages[key] = json.loads(value)
        logging.info(f"Consumed message for category: {key}")
    consumer.close()
    return messages

def ingest_news_from_kafka(**kwargs):
    ti = kwargs['ti']
    messages = ti.xcom_pull(task_ids='consume_news_from_kafka')
    spark, delta_table_base_path = get_spark_and_path()
    delta_table_base_path += "/delta_news"
    
    for category, articles in messages.items():
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            json.dump(articles, temp_file)
            file_path = temp_file.name

        df = spark.read.json(file_path)
        metadata = {"inserted_rows": len(articles)}
        metadata["perc_rows_inserted_with_null"] = get_null_percentage(df)
        
        delta_table_path = os.path.join(delta_table_base_path, category)
        os.makedirs(os.path.dirname(delta_table_path), exist_ok=True)
        df.write.mode("append") \
            .format("delta") \
            .option("userMetadata", json.dumps(metadata)) \
            .save(delta_table_path)
        os.unlink(file_path)
        logging.info(f"Ingested news for category {category} into Delta Lake at {delta_table_path}")
    
    spark.stop()
    logging.info("Spark session stopped successfully")
    
dag = DAG(
    dag_id='news_consumer_dag',
    start_date=datetime(2025, 3, 1),
    description='A DAG that consumes news messages from Kafka and writes them to Delta Lake',
    schedule_interval="@hourly",
    catchup=False
)

consume_news_task = PythonOperator(
    task_id='consume_news_from_kafka',
    python_callable=consume_news_from_kafka,
    provide_context=True,
    dag=dag
)

ingest_news_task = PythonOperator(
    task_id='ingest_news_from_kafka',
    python_callable=ingest_news_from_kafka,
    provide_context=True,
    dag=dag
)

consume_news_task >> ingest_news_task
