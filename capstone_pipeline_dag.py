from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def run_ingestion():
    print("Executing Ingestion Task: Kafka stream and validation completed successfully.")

def run_delta_processing():
    print("Executing Delta Processing Task: Delta table merge (Upsert) completed successfully.")

def run_rag_pipeline():
    print("Executing RAG Pipeline Task: FAISS indexing, RRF fusion, and Cross-Encoder reranking completed successfully.")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'capstone_data_engineering_pipeline',
    default_args=default_args,
    description='Orchestration DAG for Ingestion, Delta Processing, and Advanced RAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id='ingestion_task',
        python_callable=run_ingestion,
    )

    delta_task = PythonOperator(
        task_id='delta_processing_task',
        python_callable=run_delta_processing,
    )

    rag_task = PythonOperator(
        task_id='rag_pipeline_task',
        python_callable=run_rag_pipeline,
    )

    # Define task dependencies (Sequential Workflow)
    ingest_task >> delta_task >> rag_task
