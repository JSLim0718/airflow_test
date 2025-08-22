from airflow import DAG
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id='dags_bash_with_variable',
    schedule='25 2 * * *',
    start_date=pendulum.datetime(2025, 8, 23, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    var_value = Variable.get("sample_key")

    # Variable 라이브러리로 가져오기(비권장)
    bash_var_1 = BashOperator(
        task_id = 'bash_var_1',
        bash_command = f'echo variable:{var_value}'
    )

    # Template 활용해서 Variable 가져오기(권장)
    bash_var_2 = BashOperator(
        task_id = 'bash_var_2',
        bash_command = 'echo variable:{{var.value.sample_key}}'
    )

    bash_var_1 >> bash_var_2