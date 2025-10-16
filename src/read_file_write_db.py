import config
import pandas as pd
from mysql_db import DatabaseConnection
from pathlib import Path
from typing import Sequence


def read_sql_execute(file: Path, connection: DatabaseConnection) -> None:
    with open(file, "r") as f:
        sql = f.read()
    with connection.cursor() as cur:
        for statement in sql.split(";"):
            statement = statement.strip()
            if statement:
                cur.execute(statement)
        connection.commit()


def insert_to_db(values: Sequence, connection: DatabaseConnection) -> None:
    sql_insert_string = "INSERT INTO orders_combined (id, date_time, customer_name, customer_email, product_name, product_price) VALUES (%s, %s, %s, %s, %s, %s)"
    with connection.cursor() as cursor:
        cursor.executemany(sql_insert_string, values)
    connection.commit()


def drop_table(connection: DatabaseConnection) -> None:
    with connection.cursor() as cursor:
        cursor.execute("DROP TABLE orders_combined;")


def main():
    data = pd.read_csv(config.COMBINED_CSV)
    values = data.values.tolist()

    with DatabaseConnection(config.dbconfig) as connection:
        read_sql_execute(config.CREATE_ORDERS_COMBINED, connection)
        insert_to_db(values, connection)
        drop_table(connection)


if __name__ == "__main__":
    main()
