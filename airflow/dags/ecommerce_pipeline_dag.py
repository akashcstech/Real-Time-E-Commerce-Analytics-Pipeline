from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="ecommerce_data_pipeline",
    default_args=default_args,
    description="Kafka → Spark → Snowflake → dbt Pipeline",
    schedule_interval=None,  # or "@hourly"
    start_date=datetime(2025, 11, 1),
    catchup=False,
) as dag:

    spark_task = BashOperator(
        task_id="run_spark_ingestion",
        bash_command="bash /opt/airflow/scripts/run_spark.sh"
    )

    dbt_task = BashOperator(
        task_id="run_dbt_transformations",
        bash_command="bash /opt/airflow/scripts/run_dbt.sh"
    )

    spark_task >> dbt_task
