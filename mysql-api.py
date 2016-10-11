"""Implements an API to connect with a MySQL database.

Utilizes mysql-connector-python package.
"""
from __future__ import unicode_literals, print_function, division


# Local config.py file holding settings.
import config
from mysql import connector
from operator import itemgetter


def connect(**kwargs):
    """Return a new connection to the MySQL database."""
    try:
        return connector.connect(**kwargs)
    except connector.Error as err:
        raise err


def insert(table_name, rows):
    """Insert data into the MySQL database.

    Return number of rows inserted if successful; return failure if error.
    """
    conn = connect(**config.DEFAULT_CONFIG)
    cursor = conn.cursor()
    for row_dict in rows:
        items = row_dict.items()
        keys = map(itemgetter(0), items)
        values = map(itemgetter(1), items)

        method = 'INSERT into {}'.format(table_name)
        columns = '({})'.format(', '.join(keys))
        values = 'VALUES ({})'.format(', '.join(values))

        query_string = ' '.join((method, columns, values))
        cursor.execute(query_string)

    return


def select(table_name, columns='*', **kwargs):
    """Generate results from select call to MySQL database."""
    conn = connect(**config.DEFAULT_CONFIG)
    cursor = conn.cursor()

    method = 'SELECT {} from {}'.format(', '.join(columns), table_name)
    filters = ' AND '.join(' = '.join(pair) for pair in kwargs.items())
    where = 'WHERE {}'.format(filters)

    query_string = ' '.join((method, where))
    cursor.execute(query_string)

    column_names = cursor.column_names
    for row in cursor:
        yield dict(zip(column_names, row))


def update(*args, **kwargs):
    """Update records in MySQL database.

    Return number of rows updated if successful; return failure if error.
    """


def delete(*args, **kwargs):
    """Delete records from MySQL database.

    Return number of rows deleted if successful; return failure if error.
    """


def escape(query_string):
    """Escape characters in a query string to prevent SQL injection attacks."""
    # TODO -- implement escaping

    return query_string
