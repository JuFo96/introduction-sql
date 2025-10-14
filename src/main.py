from mysql_db import Database
from dataclasses import asdict
from pathlib import Path
import pandas as pd
import config
from mysql.connector.abstracts import MySQLCursorAbstract
import mysql.connector



def main():
    dbconfig = config.DatabaseConfig("localhost", 3306, "root", "mypassword", "db")
    with Database(dbconfig) as db:
        print(f"Database is connected: {db.is_connected()}")
        cursor = db._connection.cursor()
        print(db.connection.is_connected())
        cursor.execute("USE db")
        cursor.close()
        
        with open(config.CREATE_DB, 'r') as f:
            sql = f.read()
            
            with db.cursor() as cur:
                for statement in sql.split(';'):
                    stmt = statement.strip()    
                    if stmt:
                        cur.execute(stmt)
                db.commit()
                print(f"Database is connected: {db.is_connected()}")
                



    #cursor.execute()
    #cursor.execute(f"INSERT INTO Categories (c)")

    data = pd.read_csv(config.COMBINED_CSV)
    sql_insert_string = "INSERT INTO orders_combined (id, date_time, customer_name, customer_email, product_name, product_price) VALUES (%s, %s, %s, %s, %s, %s)"
    values = data.values.tolist()
    with mysql.connector.connect(**asdict(dbconfig)) as db:
        with db.cursor() as cursor:
            cursor.execute("USE db")
            cursor.executemany(sql_insert_string, values)
        db.commit()

    with mysql.connector.connect(host="localhost", port=3306, user="root", password="mypassword") as db:
        with db.cursor() as cur:
            cur.execute("USE db")
            cur.execute("SELECT * FROM orders_combined;")
            results = cur.fetchall()

            print("\nInserted rows:")
            for row in results:
                print(row)
        print(f"Database is connected: {db.is_connected()}")


def read_sql_execute(file: Path, connection):
    """
    """
    with open(file, 'r') as f:
        sql = f.read()
        with connection.cursor() as cur:
            cur.execute(sql)
 
if __name__ == "__main__":
    main()
