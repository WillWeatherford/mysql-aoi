"""Implements an API to connect with a MySQL database.

Utilizes mysql-connector-python package.
"""
from __future__ import unicode_literals, print_function, division


# Local config.py file holding settings.
from config import DEFAULT_CONFIG, COLS, VALID_TABLES
from mysql import connector
from flask import Flask, jsonify, request, abort


app = Flask(__name__)


def connect(**kwargs):
    """Return a new connection to the MySQL database."""
    try:
        return connector.connect(**kwargs)
    except connector.Error as err:
        raise err


@app.route("/api/<table_name>", methods=["GET", "POST", "PUT", "DELETE"])
@app.route("/api/<table_name>/<int:pk>", methods=["GET", "PUT", "DELETE"])
def endpoint(table_name, pk=None):
    """Simple get request for a single item."""
    if table_name not in VALID_TABLES:
        abort(404)

    func = globals()[request.method.lower()]
    kwargs = request.args.to_dict()

    if pk is None:
        try:
            rows = request.json.get("rows", ())
        except AttributeError:
            rows = ()
        return func(table_name, rows=rows, **kwargs)

    # Need to look up PK name from SQL instead
    pk_name = "entity_id" if table_name == "company" else "id"
    kwargs[pk_name] = pk
    rows = (kwargs, )
    return func(table_name, rows=rows, **kwargs)


def get(table_name, columns="*", rows=(), **kwargs):
    """Generate results from select call to MySQL database."""
    conn = connect(**DEFAULT_CONFIG)
    cursor = conn.cursor()

    params = []
    method = "SELECT * from {}".format(table_name)

    if not kwargs:
        query_string = method
    else:
        pairs = []
        for key, val in kwargs.items():
            try:
                pairs.append("{}=%s".format(COLS[key]))
            except KeyError:
                return jsonify(error="Bad column name: {}".format(key))
            params.append(val)

        query_string = " ".join((method, "WHERE", " AND ".join(pairs)))

    try:
        cursor.execute(query_string, params)
    except Exception as e:
        return jsonify(error="".join(e.args))

    column_names = cursor.column_names
    return jsonify(results=[dict(zip(column_names, row)) for row in cursor])


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
    conn = connect(**DEFAULT_CONFIG)
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
                        pairs.append("{}=%s".format(COLS[key]))
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
