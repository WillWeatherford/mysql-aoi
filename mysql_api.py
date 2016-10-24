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

METHODS = {
    'GET': "",
    'POST': "INSERT INTO {}",
    'PUT': "UPDATE {}",
    'DELETE': "DELETE FROM {}",
}

app = Flask(__name__)


class Connect(object):
    """Context manager for MySQL database connections."""

    def __init__(self, debug=False, **params):
        """Initialize the connection."""
        self.debug = debug
        if self.debug:
            print('Initializing Connect()')
        self.params = params

    def __enter__(self):
        """Set up the connection and return a cursor."""
        if self.debug:
            print('Begin connect __enter__')
        self.conn = connector.connect(**self.params)
        self.cursor = self.conn.cursor()
        if self.debug:
            print('Complete connect __enter__')
        return self.cursor

    def __exit__(self, *args):
        """Set up the connection."""
        if self.debug:
            print('Begin connect __exit__')
            print('Error args: {}'.format(args))
        self.conn.commit()
        self.conn.close()
        self.cursor.close()
        if self.debug:
            print('Complete connect __enter__')
        return True


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

    # conn, cursor = connect()
    # results = func(cursor, pk, pk_name, table_name, **kwargs)
    # conn.commit()
    # conn.close()
    with Connect(**config_module.CONNECT_PARAMS) as cursor:
        results = func(cursor, pk, pk_name, table_name, **kwargs)
        if not cursor.rowcount:
            abort(404)
        return jsonify(**results)


@app.route("/api/<table_name>", methods=["GET", "POST", "PUT", "DELETE"])
def endpoint_multi(table_name):
    """Route getting, posting, updating or deleting multiple rows."""
    if table_name not in config_module.VALID_TABLES:
        abort(404)
    kwargs = request.args.to_dict()

    conn, cursor = connect()
    if request.method == 'GET':
        # Figure out how to pass in criteria... json? params?
        results = get_multiple(table_name, **kwargs)
    else:
        results = post_put_delete_multi(cursor, table_name, **kwargs)
    conn.commit()
    conn.close()
    return jsonify(**results)


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


def get_multiple(columns="*", num_rows=DEFAULT_NUM_ROWS, **kwargs):
    """Return multiple rows of data, matching specified criteria."""


def post_put_delete_multi(cursor, table_name, **kwargs):
    """Insert or update based on given specifications."""
    try:
        rows = request.json["rows"]
    except (AttributeError, KeyError):
        abort(400, (
            'POST, PUT or DELETE request to this route must include json data '
            'with {"rows": [record_obj, record_obj, ...]}'
        ))

    pk_name = "entity_id" if table_name == "company" else "id"
    method = request.method
    method_str = METHODS[method].format(table_name)

    success_count = 0
    errors = []
    for row_dict in rows:
        query_parts = [method_str]
        params = []

        if method in ('PUT', 'POST'):
            set_, values = set_from_data(row_dict)
            query_parts.append(set_)
            params.extend(values)

        if method in ('PUT', 'DELETE'):
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
