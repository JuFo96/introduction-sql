from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd

import config
from connection import DatabaseConnection


def run_sql_schema(file: Path, connection: DatabaseConnection) -> None:
    with open(file, "r") as f:
        sql = f.read()
    with connection.cursor() as cur:
        for statement in sql.split(";"):
            statement = statement.strip()
            if statement:
                cur.execute(statement)
        connection.commit()


def print_iterable(iter: Iterable) -> None:
    for row in iter:
        print(row)


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
        run_sql_schema(config.CREATE_ORDERS_COMBINED, connection)
        insert_to_db(values, connection)
        drop_table(connection)


if __name__ == "__main__":
    main()
