DROP DATABASE IF EXISTS relational_db;
CREATE DATABASE relational_db;
USE relational_db;

DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;


CREATE TABLE products (
    product_id INT NOT NULL,
    product_name VARCHAR(255),
    price FLOAT,
    PRIMARY KEY (product_id)
);

CREATE TABLE customers (
    customer_id INT NOT NULL,
    customer_name VARCHAR(40),
    email VARCHAR(40),
    PRIMARY KEY (customer_id)
);

CREATE TABLE orders (
    order_id INT NOT NULL,
    timestamp DATETIME,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    PRIMARY KEY (order_id),
    CONSTRAINT fk_orders_products FOREIGN KEY (product_id) REFERENCES products(product_id) ON DELETE CASCADE,
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers(customer_id) ON DELETE CASCADE
);