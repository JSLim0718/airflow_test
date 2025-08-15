from airflow import DAG
import pendulum.datetime
from airflow.providers.standard.operators.python import PythonOperator
from common.common_func import regist2

with DAG(
    dag_id='dags_python_with_op_kwargs',
    schedule='10 3 * * *',
    start_date=pendulum.datetime(2025, 8, 16, tz='Asia/Seoul'),
    catchup=False
) as dag:
    regist2_t1 = PythonOperator(
        task_id = 'regist2_t1',
        python_callable = regist2,
        op_args = ['kr', 'seoul'],
        op_kwargs = {'email' : 'jptofcor7@naver.com', 'phone' : '010-1234-5678'}
    )

    regist2_t1