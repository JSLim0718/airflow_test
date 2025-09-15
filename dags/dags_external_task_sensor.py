from airflow import DAG
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
import pendulum
from datetime import timedelta
from airflow.utils.state import State

with DAG(
    dag_id = 'dags_external_task_sensor',
    schedule = '0 7 * * *',
    start_date = pendulum.datetime(2025, 9, 16, tz = 'Asia/Seoul'),
    catchup = False
) as dag:
    external_task_sensor_A = ExternalTaskSensor(
        task_id = 'external_task_sensor_A',
        external_dag_id = 'dags_branch_python_operator', # 외부 DAG을 센싱하기 위해 로드
        external_task_id = 'task_A',
        allowed_states = [State.SKIPPED],
        execution_delta = timedelta(hours = 6),
        poke_interval = 10 # 10초
    )

    external_task_sensor_B = ExternalTaskSensor(
        task_id = 'external_task_sensor_B',
        external_dag_id = 'dags_branch_python_operator',
        external_task_id = 'task_B',
        failed_states = [State.SKIPPED],
        execution_delta = timedelta(hours = 6),
        poke_interval = 10 # 10초
    )

    external_task_sensor_C = ExternalTaskSensor(
        task_id = 'external_task_sensor_C',
        external_dag_id = 'dags_branch_python_operator',
        external_task_id = 'task_C',
        allowed_states = [State.SUCCESS],
        execution_delta = timedelta(hours = 6),
        poke_interval = 10 # 10초
    )