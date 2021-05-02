import pymysql
import boto3
import csv 
import configparser

# Initialize a connection to the MySQL Database in the pipeline.conf
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password")

conn = pymysql.connect(host=hostname,
                        user=username,
                        passwd=password,
                        db=dbname,
                        port=int(port))

if conn is None:
    print("Xảy ra lỗi trong quá trình kết nối đến Database")
else:
    print("MySQL đã được kết nối")

m_query = "SELECT *FROM Orders;"
local_filename = "order_extract.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query)
results = m_cursor.fetchall()

with open(local_filename, 'w') as f:
    csv_w = csv.writer(f, delimiter='|')
    csv_w.writerows(results)

f.close()
m_cursor.close()
conn.close()