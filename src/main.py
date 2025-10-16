import pandas as pd

import config
import utils as utils
from config import dbconfig
from connection import DatabaseConnection
from table import Table


def main():
    with DatabaseConnection(dbconfig) as connection:
        utils.run_sql_schema(config.CREATE_RELATIONAL_DB, connection)
        orders = Table("orders", connection)
        products = Table("products", connection)
        customers = Table("customers", connection)

        orders_data = pd.read_csv(config.ORDERS_CSV)
        products_data = pd.read_csv(config.PRODUCTS_CSV)
        customers_data = pd.read_csv(config.CUSTOMERS_CSV)

        products.insertmany(products_data.to_dict("records"))
        customers.insertmany(customers_data.to_dict("records"))
        orders.insertmany(orders_data.to_dict("records"))


if __name__ == "__main__":
    main()
