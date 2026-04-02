import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

cnx = mysql.connector.connect(user=os.getenv('DB_USER'), password=os.getenv('PASSWORD'), host=os.getenv('HOST'))
cursor = cnx.cursor()

print("Connected to AWS db")

cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{os.getenv('DB_NAME')}`")
cursor.execute(f"USE `{os.getenv('DB_NAME')}`")

