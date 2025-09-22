from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum
from datetime import timedelta
from airflow.models import Variable

email_str = Variable.get("email_target")
email_1st = [email.strip() for email in email_str.split(',')]

with DAG(
    dag_id = 'dags_timeout_example_1',
    start_date = pendulum.datetime(2025, 9, 23, tz = 'Asia/Seoul'),
    catchup = False,
    schedule = None,
    dagrun_timeout = timedelta(minutes = 1),
    default_args = {
        'execution_timeout': timedelta(seconds = 20),
        'email_on_failure' : True,
        'email' : email_1st
    }
) as dag:
    bash_sleep_30 = BashOperator( # task당 timeout 기준이 20초이기 때문에 실패하는 task
        task_id = 'bash_sleep_30',
        bash_command = 'sleep 30'
    )

    bash_sleep_10 = BashOperator(
        trigger_rule = 'all_done',
        task_id = 'bash_sleep_10',
        bash_command = 'sleep 10'
    )

    bash_sleep_30 >> bash_sleep_10