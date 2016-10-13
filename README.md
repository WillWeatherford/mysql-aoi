# mysql-api
Simple Python API to interact with a MySQL database. Returns JSON data from
HTTP requests.


### Table names:
- company
- site_status
- site_status_2

### Routes:
Each route is accessible with different HTTP methods for different purposes.

- *GET* /api/<table_name>?col1=val1&col2=val2
  - Queries data from all records, filtered by column names/values provided
    as HTTP request parameters. Returns all records if no parameters are given.
  - Returns JSON data in format `{'rows': [obj1, obj2, ...]}`

- *GET* /api/<table_name>/<int>
  - Fetches record with primary key of the given int.
  - Returns JSON data in format `{'rows': [obj1, obj2, ...]}`

- *POST* /api/<table_name>
  - Creates new records. Data must be included in POST request as json in the
    format `{'rows': [obj1, obj2, ...]}`
  - Returns JSON data in format `{success: <int>}` where <int> is the number
    of records successfully inserted.

- *PUT* /api/<table_name>
  - Updates existing records. JSON data must be included in PUT request in the
    format `{'rows': [obj1, obj2, ...]}`
  - Returns JSON data in format `{success: <int>}` where <int> is the number
    of records successfully updated.

- *PUT* /api/<table_name>/<int>?col1=val1&col2=val2
  - Updates existing record with primary key of the given int, with column
    names and values provided as HTTP request parameters.
  - Returns JSON data in format `{success: <int>}` where <int> is the number
    of records successfully updated.

- *DELETE* /api/<table_name>
  - Deletes existing records. JSON data must be included in DELETE request in the
    format `{'rows': [obj1, obj2, ...]}`
  - Returns JSON data in format `{success: <int>}` where <int> is the number
    of records successfully deleted.

- *DELETE* /api/<table_name>/<int>
  - Updates existing record with primary key of the given int.
  - Returns JSON data in format `{success: <int>}` where <int> is the number
    of records successfully deleted.
