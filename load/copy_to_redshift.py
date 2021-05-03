import boto3
import configparser
import psycopg2

parser = configparser.ConfigParser()

parser.read("pipeline.conf")
dbname = parser.get("aws_creds", "database")
user = parser.get("aws_creds", "username")
password = parser.get("aws_creds", "password")
host = parser.get("aws_creds", "host")
port = parser.get("aws_creds", "port")

conn = psycopg2.connect("dbname" + dbname
                        + " user=" + user
                        + " password=" + password
                        + " host=" + host
                        + " port=" +port)

# load the account_id and iam_role from conf file

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
account_id = parser.get("aws_boto_credentials", "account_id")
iam_role = parser.get("aws_creds", "iam_role")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

# run the COPY command to load the file into Redshift
file_path = ("s3://"
            + bucket_name
            + "/order_extract.csv")
role_string = ("arn:aws:iam::"
            + account_id
            + ":role/"
            + iam_role)

sql = " COPY public.Orders"
sql = sql + " from %s"
sql = sql + " iam_role %s;"

# Now you'll need to create the table  
# run the following SQL via the Redshift Query Editor or other application connected to your cluster:
# CREATE TABLE public.Orders (
# OrderId int,
# OrderStatus varchar(30),
# LastUpdated timestamp
# );

# create a cursor object and excute the COPY
cur = conn.cursor()
cur.excute(sql, (file_path, role_string))

# close the cusor and commit transaction
cur.close()
conn.commit()

# close the connection
conn.close()