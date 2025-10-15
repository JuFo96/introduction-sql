from mysql_db import DatabaseConnection
import pandas as pd
import config
from config import dbconfig
from crud import CRUD
from typing import Iterable
from decimal import Decimal


def print_iterable(iter: Iterable) -> None:
    for row in iter:
        print(row)


def main():
    with DatabaseConnection(dbconfig) as db:
        print(f"DatabaseConnection is connected: {db.is_connected()}")
                
        with open(config.CREATE_DB, 'r') as f:
            sql = f.read()
            
            with db.cursor() as cur:
                for statement in sql.split(';'):
                    stmt = statement.strip()    
                    if stmt:
                        cur.execute(stmt)
                db.commit()
                print(f"DatabaseConnection is connected: {db.is_connected()}")
                

    data = pd.read_csv(config.COMBINED_CSV)
    sql_insert_string = "INSERT INTO orders_combined (id, date_time, customer_name, customer_email, product_name, product_price) VALUES (%s, %s, %s, %s, %s, %s)"
    values = data.values.tolist()
    with DatabaseConnection(dbconfig) as db:
        with db.cursor() as cursor:
            cursor.executemany(sql_insert_string, values)

        db.commit()

    
    cols = ["id", "date_time", "customer_name", "customer_email", "product_name", "product_price"]
    with DatabaseConnection(dbconfig) as db:
        crud = CRUD("orders_combined", db)


        results = crud.select(cols, num_rows=5)
        print_iterable(results)

        print("###################")

        crud.insert(data={"id" : 605, "customer_name" : "egan", "customer_email" : "egan@B.com"})
        results = crud.select(cols, num_rows=6)
        #print_iterable(results)
        crud.update(data={"customer_name" : "nage", "product_name": "skateboard"}, filters={"id":6})
        results = crud.select(cols, filters = {"customer_name": "Jess Stanton"})
        print_iterable(results)



 
if __name__ == "__main__":
    main()
