"""Implements an API to connect with a MySQL database.

Utilizes mysql-connector-python package.
"""
from __future__ import unicode_literals, print_function, division


# Local config.py file holding settings.
import config
from mysql import connector
from operator import itemgetter
from flask import Flask, jsonify, request

app = Flask(__name__)


@app.route('/api/<table_name>', methods=['GET'])
def select_list(table_name):
    """Simple get request for a single item."""
    results = select(table_name, **request.args)
    return jsonify(results=list(results))


@app.route('/api/<table_name>/<int:pk>', methods=['GET'])
def select_by_id(table_name, pk):
    """Simple get request for a single item."""
    pk_name = 'entity_id' if table_name == 'company' else 'id'

    results = select(table_name, criteria={pk_name: pk}, **request.args)
    obj = next(results)

    return jsonify(**obj)


@app.route('/api/<table_name>/insert', methods=['POST'])
def insert(table_name, rows=()):
    """Insert new data into the MySQL database.

    Return number of rows inserted if successful; return failure if error.
    """
    conn = connect(**config.DEFAULT_CONFIG)
    cursor = conn.cursor()

    # Probably need to do session or transaction instead
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


def connect(**kwargs):
    """Return a new connection to the MySQL database."""
    try:
        return connector.connect(**kwargs)
    except connector.Error as err:
        raise err




def select(table_name, columns='*', criteria=None):
    """Generate results from select call to MySQL database."""

    conn = connect(**config.DEFAULT_CONFIG)
    cursor = conn.cursor()

    method = 'SELECT {} from {}'.format(', '.join(columns), table_name)
    if not criteria:
        query_string = method
    else:
        filters = ' AND '.join(' = '.join(pair) for pair in criteria.items())
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
