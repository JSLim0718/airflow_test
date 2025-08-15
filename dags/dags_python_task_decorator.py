from airflow import DAG
from airflow.decorators import task
import pendulum.datetime

with DAG(
    dag_id='dags_python_task_decorator',
    schedule='26 1 * * *',
    start_date=pendulum.datetime(2025, 8, 16, tz='Asia/Seoul'),
    catchup=False
    #tags=['decorator']
) as dag:
    @task(task_id='python_task_1')
    def print_context(some_input):
        print(some_input)
        
    python_task_1 = print_context('task_decorator 실행')