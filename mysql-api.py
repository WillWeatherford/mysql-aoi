"""Implements an API to connect with a MySQL database.

Utilizes mysql-connector-python package.
"""

# Local config.py file holding settings.
import config
from mysql import connector


def connect(**kwargs):
    """Return a new connection to the MySQL database."""
    try:
        return connector.connect(**kwargs)
    except connector.Error as err:
        raise err


def insert(*args, **kwargs):
    """Insert data into the MySQL database.

    Return number of rows inserted if successful; return failure if error.
    """


def query(*args, **kwargs):
    """Generate results from select call to MySQL database."""


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
