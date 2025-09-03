from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from hooks.custom_postgres_hook import CustomPostgreshook
import pendulum
import datetime

with DAG(
    dag_id = 'dags_python_with_custom_hook_bulk_load',
    start_date = pendulum.datetime(2025, 9, 3, tz = 'Asia/Seoul'),
    schedule = '0 7 * * *',
    catchup = False
) as dag:
    
    def insert_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgreshook(postgres_conn_id = postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name = tbl_nm, file_name = file_nm, delimiter = ',', is_header = True, is_replace = True)

    insert_postgres = PythonOperator(
        task_id = 'insert_postgres',
        python_callable = insert_postgres,
        op_kwargs = {'postgres_conn_id':'conn-db-postgres-custom',
                     'tbl_nm':'tbcyclestationinfo_bulk2',
                     'file_nm':'/opt/airflow/files/tbCycleStationInfo/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tbCycleStationInfo.csv'
        }
    )

    insert_postgres()