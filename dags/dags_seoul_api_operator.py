from airflow import DAG
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
import pendulum
import datetime
with DAG(
    dag_id='dags_seoul_api_operator',
    schedule='50 6 * * *',
    start_date=pendulum.datetime(2025, 8, 26, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    '''서울시 따릉이 자전거 보관소 현황'''
    tb_cycle_count_status = SeoulApiToCsvOperator(
        task_id = 'tb_cycle_count_status',
        dataset_nm = 'tbCycleStationInfo',
        path = '/opt/airflow/files/tbCycleStationInfo/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name = 'tbCycleStationInfo.csv'
    )

    '''서울시 자전거 편의시설 현황'''
    tb_cycle_conv_status = SeoulApiToCsvOperator(
        task_id = 'tb_cycle_conv_status',
        dataset_nm = 'tvBicycleEtc',
        path = '/opt/airflow/files/tvBicycleEtc/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name = 'tvBicycleEtc.csv'
    )

    tb_cycle_count_status >> tb_cycle_conv_status
