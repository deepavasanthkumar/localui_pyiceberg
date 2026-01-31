from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime

SUB_TASKS = ["orders", "customers", "payments"]

with DAG(
    dag_id="sales_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    for sub_task in SUB_TASKS:
        SimpleHttpOperator(
            task_id=f"run_{sub_task}",
            http_conn_id="pipeline_control",
            endpoint="/pipelines/run-subtask",
            method="POST",
            headers={"Content-Type": "application/json"},
            data=f"""
            {{
              "pipeline_id": "sales_pipeline",
              "sub_task": "{sub_task}",
              "run_id": "{{{{ run_id }}}}",
              "execution_date": "{{{{ ds }}}}",
              "config": {{}}
            }}
            """
        )
