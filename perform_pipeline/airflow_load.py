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

# load the account_id and iam_role from conf file
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
account_id = parser.get(
                        "aws_boto_credentials", "account_id"
)
iam_role = parser.get("aws_creds","iam_role")

# run the COPY command to ingest tinto RedShift

file_path = "s3://bucket-name/dag_run_extract.csv"

sql = """
    COPY dag_run_history
    (id,dag_id,execution_date,
    state,run_id,external_trigger,
    end_date,start_date)
"""
sql = sql + " from %s"
sql = sql + " iam_role 'arn:aws:iam::%s:role/%s';"

# creat a cursor object and execute the COPY command
cur = rs_conn.cursor()
cur.excute(sql, (file_path, account_id, iam_role))

# close the cursor and commit the transaction
cur.close()
rs_conn.commit()
# close the connecton
rs_conn.close()