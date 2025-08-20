from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id='dags_python_with_xcom_eg2',
    schedule='30 6 * * *',
    start_date=pendulum.datetime(2025, 8, 19, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    @task(task_id='python_xcom_push_by_return')
    def xcom_push_return(**kwargs):
        return 'Success'

    @task(task_id='python_xcom_pull_1')
    def xcom_pull1(**kwargs):
        ti = kwargs['ti']
        value1 = ti.xcom_pull(task_id = 'python_xcom_push_by_return')
        print('xcom_pull 메서드로 직접 찾은 리턴 값:' + value1)
    
    @task(task_id='python_xcom_pull_2')
    def xcom_pull2(status, **kwargs):
        print('함수 입력값으로 받은 값:' + status)

    python_xcom_push_by_return = xcom_push_return()
    xcom_pull2(python_xcom_push_by_return) # python_xcom_push_by_return의 리턴값 Success
    python_xcom_push_by_return >> xcom_pull1()