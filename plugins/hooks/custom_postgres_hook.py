from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd

class CustomPostgreshook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self): # Basehook.get_connection Overriding
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port

        self.postgres_conn = psycopg2.connect(host = self.host, user = self.user, password = self.password, dbname = self.dbname, port = self.port)
        return self.postgres_conn
    
    def bulk_load(self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool):
        from sqlalchemy import create_engine

        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블:' + table_name)
        self.get_conn()
        header = 0 if is_header else None # is_header = True면 0, False면 None
        if_exists = 'replace' if is_replace else 'append' # is_replace = True면 replace, False면 append
        file_df = pd.read_csv(file_name, header = header, delimiter = delimiter)

        for col in file_df.columns:
            try:
                # string 문자열이 아닐 경우 continue
                file_df[col] = file_df[col].str.replace('\r\n', '') # string형 자료가 아니면 replace에서 에러를 산출하여 except : continue(넘어가기)
                self.log.info(f'{table_name}.{col}: 개행문자 제거')
            except:
                continue

        self.log.info('적재 건수:' + str(len(file_df)))
        uri = f'postgres://{self.user}:{self.password}@{self.host}/{self.dbname}'
        engine = create_engine(uri).raw_connection() # 판다스 2.2이상이고, sqlalchemy 1.4 이하일 때 raw_connection() 추가

        # sqlalchemy의 버전이 2.0 이상이어야 pandas 2.2 이상과 호환이 가능
        file_df.to_sql(name = table_name,
                       con = engine,
                       schema = 'public',
                       if_exists = if_exists, # 데이터를 교체할지, 증분할지를 True/False(replace, append)로 선택
                       index = False
                       )
