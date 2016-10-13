"""Implements an API to connect with a MySQL database.

Utilizes mysql-connector-python package.
"""
from __future__ import unicode_literals, print_function, division


# Local config.py file holding settings.
import config
from mysql import connector
from flask import Flask, jsonify, request, abort

app = Flask(__name__)


def connect(**kwargs):
    """Return a new connection to the MySQL database."""
    try:
        return connector.connect(**kwargs)
    except connector.Error as err:
        raise err


@app.route('/api/<table_name>', methods=['GET', 'POST', 'PUT', 'DELETE'])
@app.route('/api/<table_name>/<int:pk>', methods=['GET', 'PUT', 'DELETE'])
def endpoint(table_name, pk=None):
    """Simple get request for a single item."""
    func = globals()[request.method.lower()]
    kwargs = request.args.to_dict()

    if pk is None:
        try:
            rows = request.json.get('rows', ())
        except AttributeError:
            rows = ()
        return func(table_name, rows=rows, **kwargs)

    # Need to look up PK name from SQL instead
    pk_name = 'entity_id' if table_name == 'company' else 'id'
    kwargs[pk_name] = pk
    rows = (kwargs, )
    return func(table_name, rows=rows, **kwargs)


def get(table_name, columns='*', rows=(), **kwargs):
    """Generate results from select call to MySQL database."""
    conn = connect(**config.DEFAULT_CONFIG)
    cursor = conn.cursor()

    method = "SELECT {} from {}".format(", ".join(columns), table_name)
    if not kwargs:
        query_string = method
    else:
        filters = " AND ".join("{}='{}'".format(*pair) for pair in kwargs.items())
        where = "WHERE {}".format(filters)
        query_string = " ".join((method, where))

    try:
        cursor.execute(query_string)
    except Exception as e:
        return jsonify(error=''.join(e.args))

    column_names = cursor.column_names
    return jsonify(results=[dict(zip(column_names, row)) for row in cursor])


def post(table_name, **kwargs):
    """Insert new data into the MySQL database.

    Return number of rows inserted if successful; return failure if error.
    Requires rows to be sent in json payload as an array mapped to the 'rows'
    key.
    """
    method_str = 'INSERT INTO {}'.format(table_name)
    return post_put_delete(method_str, set_str=True, **kwargs)


def put(table_name, **kwargs):
    """Update records in MySQL database.

    Return number of rows updated if successful; return failure if error.
    """
    method_str = "UPDATE {}".format(table_name)
    pk_name = 'entity_id' if table_name == 'company' else 'id'
    return post_put_delete(method_str, pk_name=pk_name, set_str=True, **kwargs)


def delete(table_name, **kwargs):
    """Delete records from MySQL database.

    Return number of rows deleted if successful; return failure if error.
    """
    method_str = "DELETE FROM {}".format(table_name)
    pk_name = 'entity_id' if table_name == 'company' else 'id'
    return post_put_delete(method_str, pk_name=pk_name, **kwargs)


def post_put_delete(method_str, rows=(), pk_name=None, set_str=False, **kwargs):
    """Insert or update based on given specifications."""
    conn = connect(**config.DEFAULT_CONFIG)
    cursor = conn.cursor()

    success_count = 0
    try:
        for row_dict in rows:
            query_parts = [method_str]
            if set_str:
                items = ", ".join("{}='{}'".format(*pair) for pair in row_dict.items())
                query_parts.append("SET {}".format(items))
            if pk_name:
                query_parts.append("WHERE {}={}".format(pk_name, row_dict[pk_name]))
            cursor.execute(" ".join(query_parts))

            success_count += 1
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        return jsonify(error=''.join(e.args))

    return jsonify(success=success_count)


def escape(query_string):
    """Escape characters in a query string to prevent SQL injection attacks."""
    # TODO -- implement escaping

    return query_string
