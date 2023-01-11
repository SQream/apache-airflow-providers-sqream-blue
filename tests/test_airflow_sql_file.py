from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from sqream_blue.operators.sqream_blue import SQreamBlueSqlOperator

with DAG(
    dag_id='sqream_python_connector_dag4_sql_file',
    schedule_interval='0 0 * * *',
    start_date=days_ago(2),
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath=['/home/sqream/'],
    tags=['daniel test 4 sql file']
) as dag:

    list_operator = SQreamBlueSqlOperator(
        task_id='sql_file',
        sql='daniel.sql',
        sqream_blue_conn_id="daniel_connection",
        dag=dag,
    )


"""
daniel.sql -
select 1;
select 2;
select 3;
"""