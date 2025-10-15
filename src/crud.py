
from typing import Any, Iterable
import config
from mysql_db import DatabaseConnection, DatabaseConnectionConfig


class CRUD:
    """
    VALID COLS ["id", "date_time", "customer_name", "customer_email", "product_name", "product_price"]
    """
    def __init__(self, table_name: str, connection: DatabaseConnection) -> None:
        self.table_name: str = table_name
        self.connection: DatabaseConnection = connection
        self.dbconfig: DatabaseConnectionConfig = config.dbconfig
        self.valid_columns = {"id", "date_time", "customer_name", "customer_email", "product_name", "product_price"}


    def validate_columns(self, cols: Iterable[str]) -> None:
        """Validates supplied columns is in the whitelist columns to prevent sql injection

            Args:
                cols: The user supplied iterable of columns to operate on

            Raises: 
                ValueError: If the column is not in the whitelist of column names.

        """
        invalid_cols = set(cols) - self.valid_columns
        if invalid_cols:
            raise ValueError(f"Invalid column: {invalid_cols} is not in valid columns: {self.valid_columns}")

                

    def insert(self, data: dict[str, Any]) -> None:
        with self.connection.cursor() as cur:

            cols = data.keys()
            values = data.values()
            self.validate_columns(cols)

            if cols == None:
                sql_string = f"INSERT INTO {self.table_name} (id, date_time, customer_name, customer_email, product_name, product_price) VALUES (%s, %s, %s, %s, %s, %s)"
                cur.execute(sql_string, values)
            else:
                self.validate_columns(cols)

                column_string = ", ".join(cols)
                num_cols = len(cols)
                # ", ".join mimics (%s, %s, %s, %s, %s, %s) based on num_cols
                sql_string = f"INSERT INTO {self.table_name} ({column_string}) VALUES ({', '.join(['%s'] * num_cols)})"
                cur.execute(sql_string, values)
        self.connection.commit()

    def insertmany(self, values: dict[list[Any]]) -> None:
        pass

    def update(self, values: Any, cols: list[str] | None =None) -> None:
        with self.connection.cursor() as cur:
            if cols == None:
                sql_string = f"""UPDATE {self.table_name}
                SET ({set_cols}) 
                WHERE ({where_cols}) 
                VALUES (%s, %s, %s, %s, %s, %s)"""
                cur.execute(sql_string, values)
            else:
                self.validate_columns(cols)

                column_string = ", ".join(cols)
                num_cols = len(cols)
                # ", ".join mimics (%s, %s, %s, %s, %s, %s) based on num_cols
                sql_string = f"INSERT INTO {self.table_name} ({column_string}) VALUES ({', '.join(['%s'] * num_cols)})"
                cur.execute(sql_string, values)
        self.connection.commit()
                

    

    def fetch_table(self, cols: tuple | None = None, num_rows: int|None = None) -> list[any]:
        with DatabaseConnection(config) as connection:
            with connection.cursor() as cur:
                # "hack" to select all if no cols are supplied
                if not cols:
                    cur.execute(f"SELECT * FROM {table_name};")
                if cols:
                    column_string = ", ".join(cols)
                    cur.execute(f"SELECT {column_string} FROM {table_name}")
                if not num_rows:
                    results = cur.fetchall()
                else:
                    results = cur.fetchmany(num_rows)
        return results

    def update():
        pass
        
    def delete():
        pass

    def remove_table():
        pass

if __name__ == "__main__":
    crud = CRUD("orders_combined", DatabaseConnection(config.dbconfig))
    #CRUD.insert([2, 2025, 3, 20, 11, 45, 43, 'Jess Stanton', 'jess.stanton@yahoo.com', 'Mouse', '443.55'])
    crud.validate_columns(cols = ["customer_emai", "123"])