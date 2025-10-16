from typing import Any, Iterable, Optional, Sequence

from connection import DatabaseConnection


class Table:
    """
    VALID COLS ["id", "date_time", "customer_name", "customer_email", "product_name", "product_price"]
    """

    def __init__(self, table_name: str, connection: DatabaseConnection) -> None:
        self.table_name: str = table_name
        self.connection: DatabaseConnection = connection
        self.valid_columns: set = {
            "id",
            "order_id",
            "timestamp",
            "date_time",
            "customer_id",
            "customer_name",
            "customer_email",
            "product_id",
            "product_name",
            "product_price",
            "price",
            "email",
        }

    def validate_columns(self, cols: Iterable[str]) -> None:
        """Validates supplied columns is in the whitelist columns to prevent sql injection
        by applying the set difference operation.

            Args:
                cols: The user supplied iterable of columns to operate on

            Raises:
                ValueError: If the column is not in the whitelist of column names.

        """
        invalid_cols = set(cols) - self.valid_columns
        if invalid_cols:
            raise ValueError(
                f"Invalid column: {invalid_cols} is not in valid columns: {self.valid_columns}"
            )

    def insert(self, data: dict[str, Any]) -> None:
        """Inserts data dictionary into the table

        Args:
            data: the dictionary containing column names and values to insert

        Raises:
            ValueError: If the dictionary is empty
        """
        with self.connection.cursor() as cur:
            if not data:
                raise ValueError("Cannot insert empty data dictionary")

            cols = data.keys()
            self.validate_columns(cols)
            values = [data[col] for col in cols]

            column_string = ", ".join(cols)
            num_cols = len(cols)
            # ", ".join() mimics (%s, %s) based on num_cols
            sql_string = f"INSERT INTO {self.table_name} ({column_string}) VALUES ({', '.join(['%s'] * num_cols)})"
            cur.execute(sql_string, values)
        self.connection.commit()

    def insertmany(self, data: Sequence) -> None:
        with self.connection.cursor() as cur:
            if not data:
                raise ValueError("Cannot insert empty data dictionary")

            cols = data[0].keys()
            self.validate_columns(cols)
            values = []
            for row in data:
                values.append([row[col] for col in cols])

            column_string = ", ".join(cols)
            num_cols = len(cols)

            # ", ".join() mimics (%s, %s) based on num_cols
            sql_string = f"INSERT INTO {self.table_name} ({column_string}) VALUES ({', '.join(['%s'] * num_cols)})"
            cur.executemany(sql_string, values)
        self.connection.commit()

    def select(
        self,
        cols: Iterable[str],
        filters: Optional[dict[str, Any]] = None,
        limit: int | None = None,
    ) -> list[Any]:
        """_summary_

        Args:
            cols (Iterable[str]): _description_
            filters: optional dict for WHERE clause only supports "=" operator..
            limit (int | None, optional): _description_. Defaults to None.

        Raises:
            TypeError: _description_
            ValueError: _description_

        Returns:
            list[Any]: _description_
        """
        if limit is not None:
            if not isinstance(limit, int):
                raise TypeError("Limit must be an integer")
            if limit < 1:
                raise ValueError("Limit must be positive")

        with self.connection.cursor() as cur:
            self.validate_columns(cols)
            column_string = ", ".join(cols)

            sql_string = f"SELECT {column_string} FROM {self.table_name}"
            values = []

            if filters:
                self.validate_columns(filters.keys())
                where_list = [f"{col} = %s" for col in filters.keys()]
                where_string = " AND ".join(where_list)
                sql_string += f" WHERE {where_string}"
                values.extend(filters.values())

            if limit is not None:
                sql_string += " LIMIT %s"
                values.append(limit)

            cur.execute(sql_string, values)
            results = cur.fetchall()
        return results

    def update(self, data: dict[str, Any], filters: dict[str, Any]) -> None:
        """Updates the table with data dictionary with the filters dictionary supplying where clause

        Args:
            values: dictionary containing columns and values to update
            filter: dictionary containing filters. The key is a column with the value being the condition.

        Raises: ValueError if data dictionary is empty
        """

        with self.connection.cursor() as cur:
            if not data:
                raise ValueError("Cannot update: empty data dictionary")
            self.validate_columns(data)
            self.validate_columns(filters)

            set_list = [f"{col} = %s" for col in data.keys()]
            set_string = ", ".join(set_list)

            where_list = [f"{col} = %s" for col in filters.keys()]
            where_string = " AND ".join(where_list)

            values = list(data.values()) + list(filters.values())

            sql_string = (
                f"""UPDATE {self.table_name} SET {set_string} WHERE {where_string}"""
            )
            cur.execute(sql_string, values)

        self.connection.commit()

    def delete(self, filters: dict[str, Any]) -> None:
        """Deletes values from columns with the condition from filters

        Args:
            filters (dict[str, Any]): _description_

        Raises:
            ValueError: If supplied filters dict is empty
        """
        with self.connection.cursor() as cur:
            self.validate_columns(filters.keys())

            if not filters:
                raise ValueError("No condition in filters dict, cannot delete")
            else:
                where_list = [f"{col} = %s" for col in filters.keys()]
                where_string = " AND ".join(where_list)

                sql_string = f"DELETE FROM {self.table_name} WHERE {where_string}"
                values = list(filters.values())

                cur.execute(sql_string, values)
