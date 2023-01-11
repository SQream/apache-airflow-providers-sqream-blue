import logging
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqream_blue.operators.sqream_blue import SQreamBlueSqlOperator
from sqream_blue.hooks.sqream_blue import SQreamBlueHook
from airflow.utils.dates import days_ago

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with DAG(
    dag_id='sqream_python_connector_dag3',
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    tags=['daniel test 3']
) as dag:

    list_operator = SQreamBlueSqlOperator(
        task_id='create_and_insert',
        sql=['create or replace table t_a(x int not null)', 'insert into t_a values (1)', 'insert into t_a values (2)'],
        sqream_blue_conn_id="daniel_connection",
        dag=dag,
    )

    simple_operator = SQreamBlueSqlOperator(
        task_id='just_select',
        sql='select * from t_a',
        sqream_blue_conn_id="daniel_connection",
        dag=dag,
    )


    def count_python(**context):
        dwh_hook = SQreamBlueHook(sqream_blue_conn_id="daniel_connection")
        result = dwh_hook.get_first("select count(*) from public.t_a")
        logging.info("Number of rows in `public.t_a`  - %s", result[0])

    count_through_python_operator_query = PythonOperator(
        task_id="log_row_count",
        python_callable=count_python)


    list_operator >> simple_operator >> count_through_python_operator_query