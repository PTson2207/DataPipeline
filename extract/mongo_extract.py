from pymongo import MongoClient
import csv
import datetime
import boto3
from datetime import timedelta
import configparser

# load the mongo values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mongo_config", "hostname")
username = parser.get("mongo_config", "username")
password = parser.get("mongo_config", "password")
database_name = parser.get("mongo_config", "database")
collection_name = parser.get("mongo_config", "collection")

mongo_client = MongoClient(
                        "mongodb+srv://" + username
                        + ":" + password
                        + "@" + hostname
                        + "/" + database_name
                        + "?retryWrites=true&"
                        + "w=majority&ssl=true&"
                        + "ssl_cert_reqs=CERT_NONE"
)

# connect database where collection
mongo_db = mongo_client[database_name]
# choose collection
mongo_collection = mongo_db[collection_name]

start_date = datetime.datetime.today() + timedelta(days=-1)
end_date = start_date + timedelta(days=1)

mongo_query = {"$and": [{"event_timestamp": {"$gte": start_date}},
                        {"event_timestamp": {"$lt": end_date}}]}

event_docs = mongo_collection.find(mongo_query, batch_size = 3000)

# create a blank list to store the result
all_events = []

for doc in event_docs:
    event_id = str(doc.get("event_id", -1))
    event_timestamp = doc.get("event_timestamp", None)
    event_name = doc.get("event_name", None)

    # add to list
    current_event = []
    current_event.append(event_id)
    current_event.append(event_timestamp)
    current_event.append(event_name)

    all_events.append(current_event)

export_file = "export_file.csv"

with open(export_file,'w') as f:
    csv_w = csv.writer(f, delimiter='|')
    csv_w.writerows(all_events)
f.close()

# load the aws_boto_credentials values
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3 = boto3.client('s3', 
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key)

s3_file = export_file
s3.upload_file(export_file, bucket_name, s3_file)