from airflow import DAG
import pendulum
import datetime
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='dags_empty_with_edge_label',
    schedule='40 12 * * *',
    start_date=pendulum.datetime(2025, 8, 25, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    start_task = BashOperator(
        task_id = 'start_task',
        bash_command = 'echo "Start!"'
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id =  'trigger_dag_task', # 필수값
        trigger_dag_id = 'dags_python_operator', # 필수값
        trigger_run_id = None,
        #execution_date = '{{data_interval_end}}',
        conf = {
                "run_date": "{{ data_interval_end }}"
            },
        reset_dag_run = True,
        wait_for_completion = False,
        poke_interval = 60,
        allowed_states = ['success'],
        failed_states = None
    )

    start_task >> trigger_dag_task