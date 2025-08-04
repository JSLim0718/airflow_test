from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
import pendulum
import datetime

with DAG(
    dag_id="dags_bash_operator",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    tags=["예시"],
) as dag:
    bash_task_1 = BashOperator(
        task_id="bash_task_1",
        bash_command="echo whoami",
    )

    bash_task_2 = BashOperator(
        task_id="bash_task_2",
        bash_command="echo whoami",
    )

    bash_task_1 >> bash_task_2