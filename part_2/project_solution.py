from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import os

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 14, 0, 0),
}

# Визначення DAG
with DAG(
        'vlad_final_project_solution',
        default_args=default_args,
        schedule_interval=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["vlad_fp"]  # Теги для класифікації DAG
) as dag:
    landing_to_bronze = SparkSubmitOperator(
        task_id='LandingToBronze',
        application='dags/part_2/jobs/landing_to_bronze.py',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )

    bronze_to_silver = SparkSubmitOperator(
        task_id='BronzeToSilver',
        application='dags/part_2/jobs/bronze_to_silver.py',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id='SilverToGold',
        application='dags/part_2/jobs/silver_to_gold.py',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )

    # Встановлення залежностей між завданнями
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
