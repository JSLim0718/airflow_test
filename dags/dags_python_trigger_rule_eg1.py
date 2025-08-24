from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BaseOperator
from airflow.exceptions import AirflowException

with DAG(
    dag_id='dags_python_trigger_rule_eg1',
    schedule=None,
    start_date=pendulum.datetime(2025, 8, 24, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    bash_upstream_1 = BaseOperator(
        task_id = 'bash_upstream_1',
        bash_command = 'echo upstream1'
    )

    @task(task_id = 'python_upstream_1')
    def python_upstream_1():
        raise AirflowException('downstream_1 Exception!')

    @task(task_id = 'python_upstream_2')
    def python_upstream_2():
        prin('정상 처리')
    
    @task(task_id = 'python_downstream_1', trigger_rule = 'all_done')
    def python_downstream_1():
        print('정상 처리')

    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream()