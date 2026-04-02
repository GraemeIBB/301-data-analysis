import mysql.connector

cnx = mysql.connector.connect(user='root', password='12345', host='localhost')
cursor = cnx.cursor()

print("Connected to db")
