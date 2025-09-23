import pendulum
from airflow import DAG
from airflow.providers.standard.sensors.date_time import DateTimeSensorAsync

with DAG(
    dag_id = 'dags_time_sensor_with_async',
    start_date = pendulum.datetime(2025, 9, 24, 0, 0, 0),
    end_date = pendulum.datetime(2025, 9, 24, 1, 0, 0), # 0시부터 1시
    schedule = '*/10 * * * *', # 10분마다
    catchup = False
) as dag:
    sync_sensor = DateTimeSensorAsync(
        task_id = 'sync_sensor',
        target_time = """{{ macros.datetime.utcnow() + macros.timedelta(minutes=5) }}"""
    )