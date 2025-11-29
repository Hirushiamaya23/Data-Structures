"""
airflow_dag_weather_etl.py
Imports ETL functions and orchestrates them in Airflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from etl_functions import extract, transform, validate, load_daily, load_monthly

default_args = {
    "owner": "weather_team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_etl_two_file_version",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    t_extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
    )

    def run_transform(**context):
        csv = context['ti'].xcom_pull(task_ids="extract_data")
        daily, monthly = transform(csv)
        context['ti'].xcom_push(key="daily", value=daily)
        context['ti'].xcom_push(key="monthly", value=monthly)

    t_transform = PythonOperator(
        task_id="transform_data",
        python_callable=run_transform,
        provide_context=True,
    )

    def run_validate(**context):
        daily = context['ti'].xcom_pull(task_ids="transform_data", key="daily")
        validate(daily)

    t_validate = PythonOperator(
        task_id="validate_data",
        python_callable=run_validate,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    def run_load_daily(**context):
        path = context['ti'].xcom_pull(task_ids="transform_data", key="daily")
        load_daily(path)

    def run_load_monthly(**context):
        path = context['ti'].xcom_pull(task_ids="transform_data", key="monthly")
        load_monthly(path)

    t_load_daily = PythonOperator(
        task_id="load_daily",
        python_callable=run_load_daily,
        provide_context=True,
    )

    t_load_monthly = PythonOperator(
        task_id="load_monthly",
        python_callable=run_load_monthly,
        provide_context=True,
    )

    t_extract >> t_transform >> t_validate >> [t_load_daily, t_load_monthly]
