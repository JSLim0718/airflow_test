from airflow import DAG
from airflow.providers.standard.sensors.python import PythonSensor
import pendulum
from airflow.hooks.base import BaseHook

with DAG(
    dag_id = 'dags_python_sensor',
    schedule = '10 1 * * *',
    start_date = pendulum.datetime(2025, 9, 14, tz = 'Asia/Seoul'),
    catchup = False
) as dag:
    def check_api_update(http_conn_id, endpoint, base_dt_col, **kwargs):
        import requests
        import json
        import datetime
        from dateutil import relativedelta
        connection = BaseHook.get_connection(http_conn_id)
        url = f'http://{connection.host}:{connection.port}/{endpoint}/1/100/2025091413'
        response = requests.get(url)

        contents = json.loads(response.text)
        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        last_dt = row_data[0].get(base_dt_col)
        last_date = str(datetime.datetime.strptime((row_data[0].get('stationDt')[:8]), '%Y%m%d'))[:10]
        #last_date = last_date.replace(',', '-').replace('/', '-') # 현재 stationDt 컬럼의 날짜 형식 yyyyMMddhh
        try:
            pendulum.from_format(last_date, 'YYYY-MM-DD')
        except:
            from airflow.exceptions import AirflowException
            AirflowException(f'{base_dt_col} 컬럼은 YYYY.MM.DD 또는 YYYY/MM/DD 형태가 아닙니다.')
        
        today_ymd = kwargs.get('data_interval_end').in_timezone('Asia/Seoul').strftime('%Y-%m-%d')
        if last_date >= today_ymd:
            print(f'생성 확인(배치 날짜 : {today_ymd}) / API last 날짜 : {last_date}')
            return True # Complete
        else:
            print(f'update 미완료 (배치 날짜 : {today_ymd}) / API last 날짜 : {last_date}')
            return False # Reschedule
        
    sensor_task = PythonSensor(
        task_id = 'sensor_task',
        python_callable = check_api_update,
        op_kwargs = {'http_conn_id':'openapi.soeul.go.kr',
                        'endpoint':'{{var.value.apikey_openapi_seoul_go_kr}}/json/bikeListHist',
                        'base_dt_col':'stationDt'
        },
        poke_interval = 600, # 10분
        mode = 'reschedule'
    )

    sensor_task