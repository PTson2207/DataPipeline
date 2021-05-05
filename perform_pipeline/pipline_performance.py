from datetime import timedelta
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.postgres_operator import PostgresOperator 
from airflow.utils.dates import days_ago

dag = Dag(
    'pipeline_performance',
    description = 'Performance measurement pipeline',
    schedule_interval = timedelta(days=1),
    start_date = days_ago(1),
)

extract_airflow_task = BashOperator(
    task_id = 'extract_airflow',
    bash_command = 'python airflow_extract.py',
    dag = dag
)

load_airflow_task = BashOperator(
    task_id = 'load_airflow',
    bash_command = 'python airflow_load.py',
    dag = dag
)

dag_history_model_task= PostgresOperator(
    task_id = 'dag_history_model',
    postgres_conn_id = 'redshift_dw',
    sql = '/sql/dag_history_daily.sql',
    dag = dag,
)

validation_history_model_task = PostgresOperator(
    task_id = 'validation_history_model',
    postgres_conn_id = 'redshift_dw',
    sql = '/sql/validator_summary_daily.sql',
    dag = dag,
)

#scheduler
extract_airflow_task >> load_airflow_task
load_airflow_task >> dag_history_model_task
load_airflow_task >> validation_history_model_task
