"""Implements an API to connect with a MySQL database.

Utilizes mysql-connector-python package.
"""
from __future__ import unicode_literals, print_function, division

# mysql.connector.errors.IntegrityError: 1062 (23000):
#   Duplicate entry '999999999' for key 'PRIMARY'


# Local config.py file holding settings.
import os
from mysql import connector
from collections import OrderedDict
from importlib import import_module
from flask import Flask, jsonify, request, abort

DEFAULT_NUM_ROWS = 100

config_module = import_module(os.environ['MYSQL_CONFIG_MODULE'])
COLS = config_module.COLS


app = Flask(__name__)


def connect():
    """Return a new connection to the MySQL database."""
    try:
        conn = connector.connect(**config_module.CONNECT_PARAMS)
        cursor = conn.cursor()
        return conn, cursor
    except connector.Error as err:
        raise err


@app.route("/api/<table_name>/<int:pk>", methods=["GET", "PUT", "DELETE"])
def endpoint(table_name, pk):
    """Simple get request for a single item."""
    if table_name not in config_module.VALID_TABLES:
        abort(404)

    func = globals()[request.method.lower()]
    kwargs = request.args.to_dict()

    # Need to look up PK name from SQL instead
    pk_name = "entity_id" if table_name == "company" else "id"

    conn, cursor = connect()
    results = func(cursor, pk, pk_name, table_name, **kwargs)
    conn.commit()
    conn.close()
    if not cursor.rowcount:
        abort(404)
    return jsonify(**results)


@app.route("/api/<table_name>", methods=["GET", "POST", "PUT", "DELETE"])
def endpoint_multi(table_name):
    """Route getting, posting, updating or deleting multiple rows."""
    if table_name not in config_module.VALID_TABLES:
        abort(404)
    func = globals()[request.method.lower() + '_multiple']
    kwargs = request.args.to_dict()

    if request.method == 'GET':
        # Figure out how to pass in criteria... json? params?
        return func(table_name, **kwargs)

    return func(table_name, **kwargs)


def get(cursor, pk, pk_name, table_name, columns="*", **kwargs):
    """Generate result from select call to MySQL database."""
    query_str = "SELECT * from {} WHERE {}=%s".format(table_name, pk_name)

    try:
        cursor.execute(query_str, (pk, ))
    except Exception as e:
        # Return better error codes for specific errors
        return {'error': ". ".join(str(arg) for arg in e.args)}

    try:
        row = next(cursor)
    except StopIteration:
        abort(404, "Record with primary key {} not found.".format(pk))
    else:
        column_names = cursor.column_names
        return dict(zip(column_names, row))


def put(cursor, pk, pk_name, table_name, **kwargs):
    """Update single record by PK."""
    method = "UPDATE {}".format(table_name)
    set_, params = set_from_data(request.json)
    where = "WHERE {}=%s".format(pk_name)

    query_str = " ".join((method, set_, where))
    params.append(pk)

    try:
        cursor.execute(query_str, params)
    except Exception as e:
        # Return better error codes for specific errors
        return {'error': ". ".join(str(arg) for arg in e.args)}
    else:
        return {'success': 1}


def delete(cursor, pk, pk_name, table_name, **kwargs):
    """Delete single record by PK."""
    method = "DELETE FROM {}".format(table_name)
    where = "WHERE {}=%s".format(pk_name)
    query_str = " ".join((method, where))
    params = [pk]

    try:
        cursor.execute(query_str, params)
    except Exception as e:
        # Return better error codes for specific errors
        return {'error': ". ".join(str(arg) for arg in e.args)}
    else:
        return {'success': 1}

# Methods for multiple records


def get_multiple(num_rows=DEFAULT_NUM_ROWS, **kwargs):
    """Return multiple rows of data, matching specified criteria."""


def post_multiple(cursor, table_name, **kwargs):
    """Insert new data into the MySQL database.

    Return number of rows inserted if successful; return failure if error.
    Requires rows to be sent in json payload as an array mapped to the 'rows'
    key.
    """
    method_str = "INSERT INTO {}".format(table_name)
    return post_put_delete_multiple(cursor, method_str, set_str=True, **kwargs)


def put_multiple(cursor, table_name, **kwargs):
    """Update records in MySQL database.

    Return number of rows updated if successful; return failure if error.
    """
    method_str = "UPDATE {}".format(table_name)
    pk_name = "entity_id" if table_name == "company" else "id"
    return post_put_delete_multiple(cursor, method_str, pk_name=pk_name, set_str=True, **kwargs)


def delete_multiple(cursor, table_name, **kwargs):
    """Delete records from MySQL database.

    Return number of rows deleted if successful; return failure if error.
    """
    method_str = "DELETE FROM {}".format(table_name)
    pk_name = "entity_id" if table_name == "company" else "id"
    return post_put_delete_multiple(cursor, method_str, pk_name=pk_name, **kwargs)


def post_put_delete_multiple(cursor, method_str, pk_name=None, set_str=False, **kwargs):
    """Insert or update based on given specifications."""
    conn, cursor = connect()
    try:
        rows = request.json["rows"]
    except (AttributeError, KeyError):
        abort(400, (
            'POST, PUT or DELETE request to this route must include json data '
            'with {"rows": [record_obj, record_obj, ...]}'
        ))

    success_count = 0
    errors = []
    for row_dict in rows:
        params = []
        query_parts = [method_str]
        if set_str:
            set_, values = set_from_data(row_dict)
            query_parts.append(set_)
            params.extend(values)
        if pk_name:
            query_parts.append("WHERE {}=%s".format(pk_name))
            params.append(row_dict[pk_name])
        query_string = " ".join(query_parts)
        try:
            cursor.execute(query_string, params)
        except Exception as e:
            errors.append(". ".join(str(arg) for arg in e.args))
        else:
            success_count += 1
    return {'success': success_count, 'errors': errors}


def set_from_data(data):
    """Construct SET command for POST or PUT from dictionary of data."""
    try:
        data = OrderedDict(data)
    except TypeError:
        abort(400, "This request must include json data.")

    pairs = []
    for key in data.keys():
        try:
            pairs.append("{}=%s".format(COLS[key]))
        except KeyError:
            abort(400, "Bad column name: {}".format(key))

    return "SET " + ", ".join(pairs), list(data.values())
