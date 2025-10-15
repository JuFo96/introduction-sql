from mysql_db import DatabaseConnection
import pandas as pd
import config
from config import dbconfig
from crud import CRUD

def fetch_table(table_name: str, config: config.DatabaseConnectionConfig, cols: tuple | None = None, num_rows: int|None = None) -> list[any]:
    with DatabaseConnection(config) as connection:
        with connection.cursor() as cur:
            # "hack" to select all if no cols are supplied
            if not cols:
                cur.execute(f"SELECT * FROM {table_name};")
            else:
                column_string = ", ".join(cols)
                cur.execute(f"SELECT {column_string} FROM {table_name}")
            if not num_rows:
                results = cur.fetchall()
            else:
                results = cur.fetchmany(num_rows)
    return results

def print_iterable(iter: iter) -> None:
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
            cursor.executemany(sql_insert_string, values[:5])

        db.commit()


    results = fetch_table("orders_combined", dbconfig)
    #results = fetch_table("orders_combined", dbconfig, cols=("customer_name", "customer_email"), num_rows=5)
    print_iterable(results)

    print("###################")

    with DatabaseConnection(dbconfig) as db:
        crud = CRUD("orders_combined", db)
        #print(values[0])
        crud.insert([6, 'egan', 'egan@B.com'], cols=["id", "customer_name", "customer_email"])
    results = fetch_table("orders_combined", dbconfig)
    print_iterable(results)



 
if __name__ == "__main__":
    main()
