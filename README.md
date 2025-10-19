# introduction-sql
An introduction to sql, database operations and connection to python. Week 5 at specialisterne academy


## Project structure
Configuration for mysql credentials and datafiles are defined in `config.py`, sql files for creating of tables are stored in `/sql`, in `utils.py` there's a function that reads the sql file and creates the tables. `connector.py` is a wrapper class for `mysql.connector` and handles the connection between the sql server and python. `table.py` implements the create, read, update and delete methods for operating on data on the mysql server with python. `main.py` creates the tables and table object and inserts some dummy data. There's incomplete tests with pytest in `tests`
```
.
├── README.md
├── data
│   ├── customers.csv
│   ├── orders.csv
│   ├── orders_combined.csv
│   └── products.csv
├── docker-compose.yaml
├── docs
│   └── sql-introduktion.pdf
├── pyproject.toml
├── pytest.ini
├── requirements.txt
├── sql
│   ├── create_orders_combined.sql
│   └── create_relational_db.sql
├── src
│   ├── config.py
│   ├── connection.py
│   ├── main.py
│   ├── table.py
│   └── utils.py
├── tests
│   ├── integration
│   │   └── test_integration.py
│   └── unit
│       └── test_table.py
└── uv.lock
```


# Getting started
This project uses docker to host the mysql db, python to manage the data and uv to manage python. 
## Dependencies
* python >= 3.13
* mysql-connector-python>=9.4.0
* pandas>=2.3.3
* docker


## Docker

To start the MySQL server with docker run the following command, this will start a docker container detached from the current terminal.
```bash
docker compose up -d # or docker-compose up -d
```


Currently running docker contains can be inspected with 
```bash
docker ps
```
to access the mysql shell inside the docker container run the following command
```bash
docker exec -it dev_mysql mysql -u root -p
```
note that dev_mysql is the name defined in `docker-compose.yaml`

## Python
Downloading and creating a virtual environment with uv is easy
```bash
uv sync
```
alternatively pip and venv can be used with
```bash
python -m venv .venv
source .venv/bin/activate # ./.venv/Scripts/Activate on windows
pip install -r requirements.txt
```
## Running the script
```
uv run src/main.py
```
or
```
python3 src/main.py
```

## Running the tests
There's possibly need to make a manual testdb inside the mysql shell with 
```MySQL
CREATE DATABASE testdb;
```
if the author hasn't come around to fixing it yet.
```
uv run pytest
```


### TODO
* Create init script that properly setups both the relational and the combined DB, possible use environment variable or similar to chose mode.
* Create remaining unit tests and integration tests
* Refactor tables.py to make string concatenation cleaner
* Fix SQL injection vulnerability with table names
* Don't bother and just use a proper python-sql library (sqlalchemy?)