from airflow.decorators import task
from airflow import DAG

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import snowflake.connector

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def create_table():
    cur = return_snowflake_conn()
    try:
        cur.execute(f"""CREATE TABLE IF NOT EXISTS dev.raw_data.user_session_channel_hw7 (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32) default 'direct'
)""")
        cur.execute("""CREATE TABLE IF NOT EXISTS dev.raw_data.session_timestamp_hw7 (
    sessionId varchar(32) primary key,
    ts timestamp
)""")
        cur.execute("""CREATE OR REPLACE STAGE dev.raw_data.blob_stage_hw7
url = 's3://s3-geospatial/readonly/'
file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"')""")
    except Exception as e:
        print(e)
        raise e


@task
def load():
    cur = return_snowflake_conn()
    try:
        cur.execute("BEGIN;")
        cur.execute("""COPY INTO dev.raw_data.user_session_channel_hw7
FROM @dev.raw_data.blob_stage_hw7/user_session_channel.csv;""")
        cur.execute("""COPY INTO dev.raw_data.session_timestamp_hw7
FROM @dev.raw_data.blob_stage_hw7/session_timestamp.csv;""")
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'create_load_dag_2',
    catchup=False,
    tags=['ETL']
) as dag:
    create_table()
    load()


@task
def create_table():
    cur = return_snowflake_conn()
    try:
      # create session_summary table in analytics
      # duplicate removal using constraint
        cur.execute(f"""create table if not exists dev.raw_data.session_summary_hw71 (
    userId int not NULL,
    sessionId varchar(32) primary key,
    channel varchar(32),
    ts timestamp,
    CONSTRAINT unique_session UNIQUE (userId, sessionId)
)""")
  # duplicate removal using row number
        cur.execute(f"""create table if not exists dev.raw_data.session_summary_hw72 (
    userId int not NULL,
    sessionID varchar(32) primary key,
    channel varchar(32),
    ts timestamp
)""")
    except Exception as e:
        print(e)
        raise e


@task
def join_tables():
    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        # join query
        cur.execute("""insert into dev.raw_data.session_summary_hw71 (select userId,A.sessionId,channel,ts from dev.raw_data.user_session_channel_test as A join dev.raw_data.session_timestamp_test as B
on A.sessionid=B.sessionid)
        """)
        cur.execute("""insert into dev.raw_data.session_summary_hw72 (select userId,sessionID,channel,ts from (select userId,A.sessionId as sessionID,A.channel,B.ts,ROW_NUMBER() OVER (PARTITION BY userId, A.sessionId ORDER BY ts DESC) AS rn from (dev.raw_data.user_session_channel_test as A join dev.raw_data.session_timestamp_test as B
on A.sessionid=B.sessionid)) where rn=1)
        """)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise e

with DAG(
    dag_id = 'create_session_summary_table',
    catchup=False,
    tags=['ELT']
) as dag:
    create_table()
    join_tables()
