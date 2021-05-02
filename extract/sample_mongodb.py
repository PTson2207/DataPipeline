from pymongo import MongoClient
import datetime
import configparser

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mongo_config", "hostname")
username = parser.get("mongo_config", "username")
password = parser.get("mongo_config", "password")
database_name = parser.get("mongo_config", "database")

collection_name = parser.get("mongo_config", "collection")
mongo_client = MongoClient("mongodb+srv://" + username
                            + ":" + password
                            + "@" + hostname
                            + "/" + database_name
                            + "?retryWrites=true&"
                            + "w=majority&ssl=true&"
                            + "ssl_cert_reqs=CERT_NONE"
                            )

# connect to database where collection
mongo_db = mongo_client[database_name]
# choose collection to query
mongo_collection = mongo_db[collection_name]

event_1 = {
    "event_id": 1,
    "event_timestamp": datetime.datetime.today(),
    "event_name": "signup"
}
event_2 = {
    "event_id":2,
    "event_timestamp": datetime.datetime.today(),
    "event_name": "pageview"
}
event_3 = {
    "event_id": 3,
    "event_timestamp": datetime.datetime.today(),
    "event_name": "login"
}
# inset document
mongo_collection.insert_one(event_1)
mongo_collection.insert_one(event_2)
mongo_collection.insert_one(event_3)