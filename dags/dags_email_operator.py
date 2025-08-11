from airflow.sdk import DAG
import pendulum
import datetime
from airflow.providers.smtp.operators.smtp import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2025, 8, 11, tz="Asia/Seoul"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id="send_email_task", # 여기에 Airflow의 Connection에 등록한 내용 작성
        conn_id="conn_smtp_gmail",
        to="jptofcor7@gmail.com",
        subject="Airflow 성공메일",
        html_content="Airflow의 작업이 완료되었습니다."
    )
    send_email_task