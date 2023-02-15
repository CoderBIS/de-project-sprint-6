import boto3
import vertica_python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import dag
from airflow.models.variable import Variable

conn_info = {'host': '51.250.75.20',
                 'port': '5433',
                 'user': 'ANTONNN1989GMAILCOM',
                 'password': 'cOE7jZ9PMvdCRGI',
                 'database': 'dwh',
                 # Вначале он нам понадобится, а дальше — решите позже сами
                 'autocommit': True
                 }
 
vertica_conn = vertica_python.connect(**conn_info)

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")


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

def create_table_group_log():
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("""
create table if not exists ANTONNN1989GMAILCOM__STAGING.group_log
(
    group_id        int REFERENCES ANTONNN1989GMAILCOM__STAGING.groups(id),
    user_id         int REFERENCES ANTONNN1989GMAILCOM__STAGING.users(id),
    user_id_from    int,
    event           varchar(30),
    event_dt        timestamp
)
ORDER BY group_id
PARTITION BY event_dt::date
GROUP BY calendar_hierarchy_day(event_dt::date, 3, 2);
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

    create_group_log = PythonOperator(task_id='create_group_log',
                                    python_callable=create_table_group_log,
                                    op_kwargs='',
                                    dag=dag)

    upload_group_log = PythonOperator(task_id='upload_group_log',
                                    python_callable=upload_stg_group_log,
                                    op_kwargs='',
                                    dag=dag)



download_group_log >> create_group_log >> upload_group_log