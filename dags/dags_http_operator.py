from airflow import DAG
import pendulum
import datetime
from airflow.providers.http.operators.http import HttpOperator

with DAG(
    dag_id='dags_trigger_dag_run_operator',
    schedule='50 12 * * *',
    start_date=pendulum.datetime(2025, 8, 25, tz='Asia/Seoul'),
    catchup=False
) as dag:
    
    tb_cycle_station_info = HttpOperator(
        task_id = 'tb_cycle_station_info',
        http_conn_id = 'openapi.seoul.go.kr',
        endpoint = '{{var.value.apikey_openapi_seoul_go_kr}}/json/tpssRouteSectionTime/1/10/',
        method = 'GET',
        headers = {'Content-Type':'application/json',
                   'charset':'utf-8',
                   'Accept':'*/*'
        }
    )