from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk.execution_time.xcom import XCom

def parse_date(**context):
    time_tmp = datetime.strptime(str(datetime.utcnow())[:19], '%Y-%m-%d %H:%M:%S')
    kst_date = time_tmp + timedelta(hours=9)
    return str(kst_date)

with DAG (
    dag_id="dags_bash_with_template",
    schedule="50 3 * * *",
    start_date=pendulum.datetime(2025, 8, 17, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    bash_kst = PythonOperator(
        task_id = 'bash_kst',
        python_callable = parse_date
    )

    # bash_t1 = BashOperator(
    #     task_id = 'bash_t1',
    #     bash_command= 'echo "data_interval_end: {{ data_interval_end }}"'
    # )

    bash_t1 = BashOperator(
        task_id = 'bash_t1',
        bash_command= 'echo "data_interval_end: {{ ti.xcom_pull(task_ids=\'bash_kst\') }}"'
    )

    # bash_t2 = BashOperator(
    #     task_id = 'bash_t2',
    #     env = {
    #         'START_DATE' : '{{ data_interval_start | ds }}', #YYYYMMDD --> | ds
    #         'END_DATE' : '{{ data_interval_end | ds }}'
    #     },
    #     bash_command = 'echo $START_DATE && echo $END_DATE' #START_DATE 성공하면 END_DATE
    # )

    bash_t2 = BashOperator(
        task_id = 'bash_t2',
        env = {
            'START_DATE' : '{{ ti.xcom_pull(task_ids=\'bash_kst\') }}', #YYYYMMDD --> | ds
            'END_DATE' : '{{ ti.xcom_pull(task_ids=\'bash_kst\') }}'
        },
        bash_command = 'echo $START_DATE && echo $END_DATE' #START_DATE 성공하면 END_DATE
    )

    bash_kst >> bash_t1 >> bash_t2