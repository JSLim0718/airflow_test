from airflow import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
import pendulum

with DAG(
    dag_id = 'dags_files_sensor',
    schedule = '0 7 * * *',
    start_date = pendulum.datetime(2025, 9, 14, tz = 'Asia/Seoul'),
    catchup = False
) as dag:
    tbCycleStationInfo_sensor = FileSensor(
        task_id = 'tbCycleStationInfo_sensor',
        fs_conn_id = 'conn_file_opt_airflow_files',
        #filepath = 'tbCycleStationInfo/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tbCycleStationInfo.csv',
        filepath = 'tbCycleStationInfo/20250913/tbCycleStationInfo.csv',
        recursive = False,
        poke_interval = 60,
        timeout = 60*60*24, #1일, 24시간
        mode = 'reschedule'
    )

    tbCycleStationInfo_sensor