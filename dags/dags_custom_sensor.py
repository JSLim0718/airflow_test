from sensors.seoul_api_date_sensor import SeoulApiDateSensor
from airflow import DAG
import pendulum

with DAG(
    dag_id = 'dags_custom_sensor',
    start_date = pendulum.datetime(2025, 9, 18, tz = 'Asia/Seoul'),
    schedule = None,
    catchup = False
) as dag:
    tb_bike_list_hist_sensor = SeoulApiDateSensor(
        task_id = 'tb_bike_list_hist_sensor',
        dataset_nm = 'bikeListHist',
        base_dt_col = 'stationDt',
        day_off = -1,
        poke_interval = 600,
        mode = 'reschedule'
    )