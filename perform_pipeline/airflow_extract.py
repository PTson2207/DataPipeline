import csv
import boto3
import configparser
import psycopg2

# get db RedShift connection info
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("aws_creds", "database")
user = parser.get("aws_creds", "username")
password = parser.get("aws_creds", "password")
host = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")

# connect to redshift cluster
rs_conn = psycopg2.connect(
                        "dbname=" + dbname
                        + " user=" + user
                        + " passord=" + password
                        + " host=" + host
                        + " port=" + port
)

rs_sql = """
        SELECT COALESCE(MAX(id), -1)
        FROM dag_run_history;
"""
# dag_run_history creat from sql 
