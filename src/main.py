import config
import utils
from connection import DatabaseConnection
from table import Table


def main():
    with DatabaseConnection(config.dbconfig) as connection:
        utils.run_sql_schema(config.CREATE_RELATIONAL_DB, connection)
        orders = Table("orders", connection)
        products = Table("products", connection)
        customers = Table("customers", connection)

        orders_data = utils.load_csv_to_dict(config.ORDERS_CSV)
        products_data = utils.load_csv_to_dict(config.PRODUCTS_CSV)
        customers_data = utils.load_csv_to_dict(config.CUSTOMERS_CSV)

        products.insertmany(products_data)
        customers.insertmany(customers_data)
        orders.insertmany(orders_data)

        result = products.select(["*"], filters={"product_name": "Laptop"})
        utils.print_iterable(result)
        products.delete({"product_name": "Laptop"})
        result = products.select(["*"], filters={"product_name": "Laptop"})
        utils.print_iterable(result)


if __name__ == "__main__":
    main()
