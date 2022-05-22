import mysql.connector

def sql_connector():
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="password",
    database='prefect_test'
    )

    mycursor = mydb.cursor()
    return mycursor, mydb
