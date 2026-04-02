import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

def connect_db():
    cnx = mysql.connector.connect(user=os.getenv('DB_USER'), password=os.getenv('PASSWORD'), host=os.getenv('HOST'))
    cursor = cnx.cursor()

    print("Connected to AWS db")

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{os.getenv('DB_NAME')}`")
    cursor.execute(f"USE `{os.getenv('DB_NAME')}`")

    return cnx


def create_tables(cursor):
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



def insert_into_table1(cursor, df1):
    table1_insert = "INSERT INTO interprovincial_tourist_spending VALUES (%s, %s, %s, %s)"
    rows = [tuple(row) for row in df1.itertuples(index=False)]
    cursor.executemany(table1_insert, rows)
    print(f"Inserted {cursor.rowcount} rows")

def insert_into_table2(cursor, df2):
    table2_insert = "INSERT INTO international_tourist_spending VALUES (%s, %s, %s, %s, %s)"
    rows = [tuple(row) for row in df2.itertuples(index=False)]
    cursor.executemany(table2_insert, rows)
    print(f"Inserted {cursor.rowcount} rows")

def insert_into_table3(cursor, df3):
    table3_insert = "INSERT INTO tourism_supply_demand VALUES (%s, %s, %s, %s, %s)"
    rows = [tuple(row) for row in df3.itertuples(index=False)]
    cursor.executemany(table3_insert, rows)
    print(f"Inserted {cursor.rowcount} rows")

def insert_into_table4(cursor, df4):
    table4_insert = "INSERT INTO provincial_visitor_count VALUES (%s, %s, %s, %s)"
    split_size = 1000
    rows = [tuple(row) for row in df4.itertuples(index=False)]

    for i in range(0, len(rows), split_size):
        split = rows[i : i + split_size]
        cursor.executemany(table4_insert, split)
    
    print(f"Inserted {cursor.rowcount} rows")

