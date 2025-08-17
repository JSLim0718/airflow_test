from airflow import DAG
import pendulum
import datetime
from airflow.providers.standard.operators.python import PythonOperator
from airflow.decorators import task

with DAG (
    dag_id='dags_python_templates',
    schedule='45 22 * * *',
    start_date=pendulum.datetime(2025, 8, 17, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    def python_function(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    python_t1 = PythonOperator(
        task_id = 'python_t1',
        python_callable = python_function,
        op_kwargs = {"start_date" : "{{ data_interval_start | ds }}", "end_date" : "{{ data_interval_end | ds }}"}
    )
    
    @task(task_id = 'python_t2') #task 데코레이터
    def python_function2(**kwargs):
        print(kwargs)
        print('ds :' + kwargs['ds'])
        print('ts :' + kwargs['ts'])
        print('data_interval_start :' + str(kwargs['data_interval_start']))
        print('data_interval_end :' + str(kwargs['data_interval_end']))
        print('task_instance :' + str(kwargs['ti']))

    python_t1 >> python_function2()