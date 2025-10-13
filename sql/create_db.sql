DROP DATABASE IF EXISTS db;
CREATE DATABASE IF NOT EXISTS db;

CREATE TABLE `orders_combined` (
    `id` INTEGER NOT NULL AUTO_INCREMENT,
    `date_time` DATETIME,
    `customer_name` VARCHAR(40),
    `customer_email` VARCHAR(40),
    `product_name` VARCHAR(255),
    `product_price` DECIMAL(10, 2),
     PRIMARY KEY (`id`)
);