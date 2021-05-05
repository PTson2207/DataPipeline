from datetime import timedelta
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.postgres_operator import PostgresOperator 
from airflow.utils.dates import days_ago