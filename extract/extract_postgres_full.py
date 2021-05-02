import psycopg2
import csv
import boto3
import configparser

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("postgres_config", "database")
user = parser.get("postgres_config", "username")
password = parser.get("postgres_config", "password")
host = parser.get("postgres_config", "host")
port = parser.get("postgres_config", "port")

conn = psycopg2.connect(
    "dbname" + dbname
    + " user=" + user
    + " password=" + password
    + " host=" + host,
    port = port 
)
if conn is None:
    print("Xảy ra lỗi trong quá trình kết nối đến Database")
else:
    print("Kết nối đến PostgreSQL thành công")

m_query = "SELECT *FROM Orders;"
local_filename = "order_etract.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query)
results = m_cursor.fetchall()

with open(local_filename, 'w') as f:
    csv_w = csv.writer(f, delimiter="|")
    csv_w.writerows(results)

f.close()
m_cursor.close()
conn.close()

# load the aws_boto_credentials values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3 = boto3.client('s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key)
s3_file = local_filename
s3.upload_file(local_filename,
                bucket_name,
                s3_file)