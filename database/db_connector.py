import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

cnx = mysql.connector.connect(user=os.getenv('DB_USER'), password=os.getenv('PASSWORD'), host=os.getenv('HOST'))
cursor = cnx.cursor()

print("Connected to AWS db")

cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{os.getenv('DB_NAME')}`")
cursor.execute(f"USE `{os.getenv('DB_NAME')}`")

create_table1_statement = 'CREATE TABLE IF NOT EXISTS interprovincial_tourist_spending ' \
'(year SMALLINT, province_of_residence VARCHAR(60), destination_province VARCHAR(60),' \
'amount_spent DECIMAL(20,1))'

cursor.execute(create_table1_statement)

create_table2_statement = 'CREATE TABLE IF NOT EXISTS international_tourist_spending ' \
'(date VARCHAR(10), region_visited VARCHAR(80), place_of_residence VARCHAR(60),' \
'expenditure_type VARCHAR(70), amount_spent DECIMAL(20,1))'

cursor.execute(create_table2_statement)

create_table3_statement = 'CREATE TABLE IF NOT EXISTS tourism_supply_demand ' \
'(year SMALLINT, province VARCHAR(60), economic_measure VARCHAR(60),' \
'product VARCHAR(80), value_in_millions DECIMAL(15,1))'

cursor.execute(create_table3_statement)

create_table4_statement = 'CREATE TABLE IF NOT EXISTS provincial_visitor_count ' \
'(date VARCHAR(10), destination_province VARCHAR(60), place_of_residence VARCHAR(60),' \
'visitor_count INT)'

cursor.execute(create_table4_statement)

cursor.execute('SHOW TABLES')

for table in cursor:
    print(table)