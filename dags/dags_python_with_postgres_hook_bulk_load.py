from airflow import DAG
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id = 'dags_python_with_postgres_hook_bulk_load',
    schedule = '0 7 * * *',
    start_date = pendulum.datetime(2025, 9, 1, tz = 'Asia/Seoul'),
    catchup = False
) as dag:
    
    def insert_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        postgres_hook = PostgresHook(postgres_conn_id)
        postgres_hook.bulk_load(tbl_nm, file_nm)

    insert_postgres = PythonOperator(
        task_id = 'insert_postgres',
        python_callable = insert_postgres,
        op_kwargs = {
            'postgres_conn_id':'conn-db-postgres-custom',
            'tbl_nm':'tbCycleStationInfo_bulk1',
            'file_nm':'/opt/airflow/files/tbCycleStationInfo/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tbCycleStationInfo.csv'
        }
    )