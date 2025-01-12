from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

def create_spark_submit_task(dag, task_id, application, application_args=None):
    return SparkSubmitOperator(
        task_id=task_id,
        application=application,
        application_args=application_args or [],
        conn_id='spark_default',
        verbose=True,
        dag=dag
    )
def create_spark_submit_task(dag, task_id, application, application_args=None):
    return SparkSubmitOperator(
        task_id=task_id,
        application=application,
        application_args=application_args or [],
        conn_id='spark_default',
        verbose=True,
        jars="/usr/local/airflow/plugins/postgresql-42.4.4.jar",  # Add the correct path to your PostgreSQL JDBC driver
        driver_class_path="/usr/local/airflow/plugins/postgresql-42.4.4.jar",  # Same path as above
        dag=dag
    )
def create_bash_task(dag, task_id, bash_command):
    return BashOperator(
        task_id=task_id,
        bash_command=bash_command,
        dag=dag
    )

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1}

# Define the DAG
with DAG(
    'medallion_architecture_dag',
    default_args=default_args,
    description='Orchestrate the Medallion Architecture for Brazilian E-commerce Data',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:

    # Bronze Layer: Load raw data into PostgreSQL (Bronze DB)
    bronze_task = create_spark_submit_task(
        dag=dag,
        task_id='bronze_layer',
        application='./include/scripts/to_bronze.py',
    )

    # Silver Layer: Perform joins and transformations (Silver DB)
    silver_task = create_spark_submit_task(
        dag=dag,
        task_id='silver_layer',
        application='./include/scripts/to_silver.py',
    )

    # Gold Layer: Generate analytics and metrics (Gold DB)
    gold_task = create_spark_submit_task(
        dag=dag,
        task_id='gold_layer',
        application='./include/scripts/to_gold.py',
    )

    # Data Validation Tasks (Optional)
    validate_gold_data = create_bash_task(
        dag=dag,
        task_id='validate_gold_data',
        bash_command='echo "Validating data in Gold layer"'
    )

    # Define task dependencies
    bronze_task >> silver_task >> gold_task >> validate_gold_data
