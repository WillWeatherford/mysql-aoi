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

DEFAULT_NUM_ROWS = 20
MAX_NUM_ROWS = 100

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
            print('Complete connect __exit__')
        return False


##########################
# Routes

@app.route("/api/<table_name>/<int:pk>", methods=["GET", "PUT", "DELETE"])
def endpoint(table_name, pk):
    """Simple get, put or delete request for a single item."""
    if table_name not in config_module.VALID_TABLES:
        abort(404)

    kwargs = request.args.to_dict()

    with Connect(**config_module.CONNECT_PARAMS) as cursor:
        if request.method == 'GET':
            # Figure out how to pass in criteria... json? params?
            results = get(cursor, pk, table_name, **kwargs)
        else:
            results = post_put_delete(cursor, pk, table_name, **kwargs)
        if not cursor.rowcount:
            abort(404, "{} not found by primary key {}".format(table_name, pk))
        return jsonify(**results)


@app.route("/api/<table_name>", methods=["GET", "POST", "PUT", "DELETE"])
def endpoint_multi(table_name):
    """Route getting, posting, updating or deleting multiple rows."""
    if table_name not in config_module.VALID_TABLES:
        abort(404)
    kwargs = request.args.to_dict()

    with Connect(**config_module.CONNECT_PARAMS) as cursor:
        if request.method == 'GET':
            # Figure out how to pass in criteria... json? params?
            results = get_multi(cursor, table_name, **kwargs)
        elif request.method == 'POST' and not request.json.get('rows'):
            results = post_put_delete(cursor, None, table_name, **kwargs)
        else:
            results = post_put_delete_multi(cursor, table_name, **kwargs)
        return jsonify(**results)


#####################
# Single item methods

def get(cursor, pk, table_name, columns=None, **kwargs):
    """Generate result from select call to MySQL database."""
    pk_name = "entity_id" if table_name == "company" else "id"
    if columns:
        columns_str = make_columns_str(columns)
    else:
        columns_str = "*"

    query_str = "SELECT {} from {} WHERE {}=%s".format(
        columns_str, table_name, pk_name
    )
    try:
        cursor.execute(query_str, (pk, ))
    except Exception:
        # Return better error codes for specific errors
        abort(500, "Something went wrong with your query.")

    try:
        row = next(cursor)
    except StopIteration:
        abort(404, "Record with primary key {} not found.".format(pk))
    else:
        column_names = cursor.column_names
        return dict(zip(column_names, row))


def post_put_delete(cursor, pk, table_name, **kwargs):
    """Create, update or delete a record."""
    pk_name = "entity_id" if table_name == "company" else "id"
    method = request.method
    method_str = METHODS[method].format(table_name)
    query_parts = [method_str]
    params = []

    if method in ('PUT', 'POST'):
        set_, values = set_from_data(request.json)
        query_parts.append(set_)
        params.extend(values)

    if method in ('PUT', 'DELETE'):
        query_parts.append("WHERE {}=%s".format(pk_name))
        params.append(pk)

    query_str = " ".join(query_parts)

    try:
        cursor.execute(query_str, params)
    except Exception:
        # Return better error codes for specific errors
        abort(500, "Something went wrong with your query.")
    else:
        return {'success': 1}


##############################
# Multiple item methods

def get_multi(cursor, table_name, columns=None, **kwargs):
    """Return multiple rows of data, matching specified criteria."""
    data = request.json or {}

    num_rows = int(data.get('num_rows', DEFAULT_NUM_ROWS))
    if num_rows > MAX_NUM_ROWS:
        abort(400, "Maximum num_rows in GET request: {}".format(MAX_NUM_ROWS))

    params = []
    columns_str = make_columns_str(data.get('columns'))
    query_str = "SELECT {} FROM {}".format(columns_str, table_name)

    criteria = data.get('criteria')
    if criteria:
        criteria_str, values = make_criteria_str(criteria)
        query_str = " ".join((query_str, criteria_str))
        params.extend(values)

    query_str += " LIMIT %s"
    params.append(num_rows)

    try:
        cursor.execute(query_str, params)
    except Exception:
        # Return better error codes for specific errors
        abort(500, "Something went wrong with your query.")

    column_names = cursor.column_names
    return {'rows': [dict(zip(column_names, row)) for row in cursor]}


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

    if len(rows) > MAX_NUM_ROWS:
        abort(400, "Maximum rows in {} request: {}".format(method, MAX_NUM_ROWS))

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
        except Exception:
            errors.append(row_dict)
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


def make_columns_str(columns):
    """Make a column string to be included in a SELECT query."""
    if not columns:
        return "*"
    invalid = set(COLS) - set(columns)
    if invalid:
        abort(400, "Invalid column names. {}".format(', '.join(invalid)))
    return ", ".join(columns)


def make_criteria_str(criteria):
    """Make a filters for WHERE query."""
    try:
        data = OrderedDict(criteria)
    except TypeError:
        abort(400, "Badly formatted criteria.")

    pairs = []
    for key in data.keys():
        try:
            pairs.append("{}=%s".format(COLS[key]))
        except KeyError:
            abort(400, "Bad column name in criteria: {}".format(key))

    return "WHERE " + " AND ".join(pairs), list(data.values())
