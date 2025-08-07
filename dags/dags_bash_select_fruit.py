from airflow import DAG
import pendulum
import datetime
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="10 0 * * 6#1", #첫 번째 토요일 0시 10분마다
    start_date=pendulum.datetime(2025, 8, 7, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    task1_orange = BashOperator(
        task_id="t1_orange"
        bash_command="/opt/airflow/plugins/shell/fruit.sh ORANGE"
    )
    task2_grape = BashOperator(
        task_id="t2_grape"
        bash_command="/opt/airflow/plugins/shell/fruit.sh GRAPE"
    )
    task3_apple = BashOperator(
        task_id="t3_apple"
        bash_command="/opt/airflow/plugins/shell/fruit.sh APPLE"
    )
    task4_jslim = BashOperator(
        task_id="t4_jslim"
        bash_command="/opt/airflow/plugins/shell/fruit.sh JSLIM"
    )

    task1_orange >> task2_grape >> task3_apple >> task4_jslim