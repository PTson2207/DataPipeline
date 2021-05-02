import pymysql
import boto3
import csv 
import configparser

# Initialize a connection to the MySQL Database in the pipeline.conf
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")