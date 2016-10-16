"""Implements an API to connect with a MySQL database.

Utilizes mysql-connector-python package.
"""
from __future__ import unicode_literals, print_function, division

# mysql.connector.errors.IntegrityError: 1062 (23000):
#   Duplicate entry '999999999' for key 'PRIMARY'


# Local config.py file holding settings.
import os
from mysql import connector
from flask import Flask, jsonify, request, abort
from importlib import import_module

config_module = import_module(os.environ['MYSQL_CONFIG_MODULE'])


app = Flask(__name__)


def connect(**kwargs):
    """Return a new connection to the MySQL database."""
    try:
        return connector.connect(**kwargs)
    except connector.Error as err:
        raise err


@app.route("/api/<table_name>", methods=["GET", "POST", "PUT", "DELETE"])
def endpoint_multi(table_name):
    """Route for requests getting, posting or updating multiple rows."""
    if table_name not in config_module.VALID_TABLES:
        abort(404)
    func = globals()[request.method.lower()] + '_multiple'
    kwargs = request.args.to_dict()

    if request.method == 'GET':
        # Figure out how to pass in criteria... json? params?
        return func(table_name, **kwargs)

    try:
        rows = request.json["rows"]
    except (AttributeError, KeyError):
        abort(400, (
            'POST, PUT or DELETE request to this route must include json data '
            'with {"rows": [record_obj, record_obj, ...]}'
        ))
    return func(pk, table_name, rows=rows, **kwargs)


@app.route("/api/<table_name>/<int:pk>", methods=["GET", "PUT", "DELETE"])
def endpoint(table_name, pk):
    """Simple get request for a single item."""
    if table_name not in config_module.VALID_TABLES:
        abort(404)

    func = globals()[request.method.lower()]
    kwargs = request.args.to_dict()

    # Need to look up PK name from SQL instead
    return func(table_name, **kwargs)


def get(pk, table_name, columns="*", **kwargs):
    """Generate results from select call to MySQL database."""
    conn = connect(**config_module.CONNECT_PARAMS)
    cursor = conn.cursor()
    pk_name = "entity_id" if table_name == "company" else "id"

    query_str = "SELECT * from {} WHERE {}=%s".format(table_name, pk_name)

    try:
        cursor.execute(query_str, (pk, ))
    except Exception as e:
        # Return better error codes for specific errors
        return jsonify(error="".join(e.args))

    try:
        row = next(cursor)
    except StopIteration:
        abort(404, "Record with primary key {} not found.".format(pk))
    else:
        column_names = cursor.column_names
        return jsonify(dict(zip(column_names, row)))


def get_multiple():
    """Return multiple rows of data, matching specified criteria."""


def post(table_name, **kwargs):
    """Insert new data into the MySQL database.

    Return number of rows inserted if successful; return failure if error.
    Requires rows to be sent in json payload as an array mapped to the 'rows'
    key.
    """
    method_str = "INSERT INTO {}".format(table_name)
    return post_put_delete(method_str, set_str=True, **kwargs)


def put(table_name, **kwargs):
    """Update records in MySQL database.

    Return number of rows updated if successful; return failure if error.
    """
    method_str = "UPDATE {}".format(table_name)
    pk_name = "entity_id" if table_name == "company" else "id"
    return post_put_delete(method_str, pk_name=pk_name, set_str=True, **kwargs)


def delete(table_name, **kwargs):
    """Delete records from MySQL database.

    Return number of rows deleted if successful; return failure if error.
    """
    method_str = "DELETE FROM {}".format(table_name)
    pk_name = "entity_id" if table_name == "company" else "id"
    return post_put_delete(method_str, pk_name=pk_name, **kwargs)


def post_put_delete(method_str, rows=(), pk_name=None, set_str=False, **kwargs):
    """Insert or update based on given specifications."""
    conn = connect(**config_module.CONNECT_PARAMS)
    cursor = conn.cursor()

    params = []

    success_count = 0
    try:
        for row_dict in rows:
            query_parts = [method_str]
            if set_str:
                pairs = []
                for key, val in row_dict.items():
                    try:
                        pairs.append("{}=%s".format(config_module.COLS[key]))
                    except KeyError:
                        return jsonify(error="Bad column name: {}".format(key))
                    params.append(val)
                query_parts.append("SET")
                query_parts.append(", ".join(pairs))
            if pk_name:
                query_parts.append("WHERE {}=%s".format(pk_name))
                params.append(row_dict[pk_name])
            query_string = " ".join(query_parts)
            cursor.execute(query_string, params)

            success_count += 1
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        return jsonify(error="".join(e.args))

    return jsonify(success=success_count)
