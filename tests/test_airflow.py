from airflow import settings
from airflow.models import Connection
from sqream_blue.operators.sqream_blue import SQreamBlueSqlOperator
from airflow import DAG
import datetime

conn_id = "daniel_connection"
conn_type = "sqream"
host = "danielg.isqream.com"
login = "sqream"
password = "sqream"
port = 443




#connect to airflow
conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        login=login,
        password=password,
        port=port
) #create a connection object
session = settings.Session() # get the session
session.add(conn)
session.commit() # it will insert the connection object programmatically.



# connect to sqream




args = {"owner": "Airflow", "start_date": datetime(2022,1,1,1,1)}

dag = DAG(
    dag_id="sqreamm_dag", default_args=args, schedule_interval=None
)

simple_operator = SQreamBlueSqlOperator(
    task_id="just_select",
    sql='select * from t',
    sqream_conn_id="daniel_connection")

list_operator = SQreamBlueSqlOperator(
    task_id="create_and_insert",
    sql=['create table t(x int not null)', 'insert into t values (1)'],
    sqream_conn_id="daniel_connection")


list_operator >> simple_operator