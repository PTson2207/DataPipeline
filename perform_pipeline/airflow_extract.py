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
rs_cursor = rs_conn.cursor()
rs_cursor.excute(rs_sql)
results = rs_cursor.fetchone()

# return only one row and column
last_id = results[0]
rs_cursor.close()
rs_conn.commit()

# connect to airflow
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("airflow_config", "database")
user = parser.get("airflow_config", "username")
password = parser.get("airflow_config", "password")
host = parser.get("airflow_config", "host")
port = parser.get("airflow_config", "port")

conn = psycopg2.connect(
                "dbname=" + dbname
                + " user=" + user
                + " passrowd=" + password
                + " host=" + host
                + " port=" + port
)

m_query = """
            SELECT
                id,
                dag_id,
                execution_date,
                state,
                run_id,
                external_trigger,
                end_date,
                start_date
            FROM dag_run
            WHERE id > %s
            AND state <> \'running\';
"""

m_cursor = conn.cursor()
m_cursor.excute(m_query, (last_id,))
results = m_cursor.fetchall()

local_filename = "dag_run_extract.csv"
with open(local_filename, "w") as f:
    csv_w = csv.writer(f, delimiter='|')
    csv_w.writerows(results)
f.close()
m_cursor.close()
conn.close()

#load the aws_boto_credentials calues
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

# upload the local to the S3 buket
s3 = boto3.client(
    's3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key
)

s3_file = local_filename
s3.upload_file(local_filename, bucket_name, s3_file)