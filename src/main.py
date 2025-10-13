from mysql_db import MySqlDBConnection
from pathlib import Path
import pandas as pd
import config
from mysql.connector.abstracts import MySQLCursorAbstract
import mysql.connector

def main():
    print("Hello from introduction-sql!")
    db = mysql.connector.connect(host="localhost", port=3306, user="root", password="mypassword")
    print(f"Database is connected: {db.is_connected()}")
    cursor = db.cursor()
    #with db.cursor() as cursor:
    #    read_sql_execute(config.CREATE_DB, cursor)
    #cursor.fetchall()
    cursor = read_sql_execute(config.CREATE_DB, cursor, db)

    #cursor.execute()
    #cursor.execute(f"INSERT INTO Categories (c)")

    data = pd.read_csv(config.COMBINED_CSV)
    sql_insert_string = "INSERT INTO orders_combined (id, date_time, customer_name, customer_email, product_name, product_price) VALUES (%s, %s, %s, %s, %s, %s)"
    values = data.values.tolist()
    #cursor.reset()
    
    cursor.executemany(sql_insert_string, values)
    #cursor.fetchall()


        #cursor.execute(f"INSERT INTO Categories")
    #data.to_sql(name="Categories", if_exists="append", index=False) 
    #row = cursor.fetchall()
    #print(row)

def read_sql_execute(file: Path, cursor: MySQLCursorAbstract, connection) -> MySQLCursorAbstract:
    """
    """
    with open(file, 'r') as f:
        sql = f.read()
        cursor.execute(sql)
        connection.commit()
    
    return cursor





if __name__ == "__main__":
    main()
