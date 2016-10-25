"""Example local config for MySQL connection."""

HOST = '00.00.00.00'
USER = 'username'
PASSWORD = 'password'
DATABASE = 'dbname'

CONNECT_PARAMS = {
    'host': HOST,
    'user': USER,
    'password': PASSWORD,
    'database': DATABASE,
}

VALID_TABLES = {'table1', 'table2', 'table3'}
VALID_COLUMN_NAMES = [
    'col1',
    'col2',
    'table1_col1',
    'table2_col2',
]
COLS = dict(zip(VALID_COLUMN_NAMES, VALID_COLUMN_NAMES))
