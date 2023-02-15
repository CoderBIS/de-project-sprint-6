import boto3
import vertica_python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag
from airflow.models.variable import Variable


AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
LOGIN = Variable.get("LOGIN")
PASSWORD = Variable.get("PASSWORD")

conn_info = {'host': '51.250.75.20',
                 'port': '5433',
                 'user': LOGIN,
                 'password': PASSWORD,
                 'database': 'dwh',
                 'autocommit': True
                 }
 
vertica_conn = vertica_python.connect(**conn_info)


def get_group_log():
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )
    s3_client.download_file(
        Bucket='sprint6',
        Key='group_log.csv',
        Filename='/data/group_log.csv'
        )


def truncate_table_group_log():
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("""
TRUNCATE TABLE ANTONNN1989GMAILCOM__STAGING.group_log;
""")


def upload_stg_group_log():
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("""
COPY ANTONNN1989GMAILCOM__STAGING.group_log (group_id, user_id, user_id_from, event, event_dt)
FROM LOCAL '/data/group_log.csv'
DELIMITER ','
REJECTED DATA AS TABLE ANTONNN1989GMAILCOM__STAGING.group_log_rej;
""")




with DAG(
    dag_id='dag_get_group_log',
    schedule_interval=None,
    start_date=datetime.today(),
    catchup=False
) as dag:

    download_group_log = PythonOperator(task_id='download_group_log',
                                    python_callable=get_group_log,
                                    op_kwargs='',
                                    dag=dag)

    truncate_group_log = PythonOperator(task_id='truncate_group_log',
                                    python_callable=truncate_table_group_log,
                                    op_kwargs='',
                                    dag=dag)

    upload_group_log = PythonOperator(task_id='upload_group_log',
                                    python_callable=upload_stg_group_log,
                                    op_kwargs='',
                                    dag=dag)



download_group_log >> truncate_group_log >> upload_group_log