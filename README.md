# mysql-api
Simple Python API to interact with a MySQL database. Returns JSON data from
HTTP requests.

### Setup:
- create config.py file -- see example_config.py
From command line:
- pip install -r requirements.txt
- export FLASK_APP = mysql_api.py
- flask run

### Routes:
Each route is accessible with different HTTP methods for different purposes.

- *GET* `/api/<table_name>`
  - Queries data from all records.
  - Takes parameters included as json:
    - columns: a list of columns to include in returned data
    - criteria: a JSON object of column/value pairs to match
    - num_rows: the number of rows to return. Default=20, maximum=100
  - Returns first records if no parameters are given.
  - Returns JSON data in format `{'rows': [obj1, obj2, ...]}`

- *GET* `/api/<table_name>/<int>`
  - Fetches record with primary key of the given int.
  - Returns JSON data in format `{'rows': [obj]}`

- *POST* `/api/<table_name>`
  - Creates new records.
  - Data to insert must be included in POST request as json for a single record,
    and the format `{'rows': [obj1, obj2, ...]}` for multiple records.
  - Returns JSON data in format `{'success': <int>, 'errors': []}`
    where <int> is the number of records successfully inserted.

- *PUT* `/api/<table_name>`
  - Updates existing records. JSON data must be included in PUT request in the
    format `{'rows': [obj1, obj2, ...]}`
  - Each row must include a primary key column and value.
  - Returns JSON data in format `{success: <int>}` where <int> is the number
    of records successfully updated.

- *PUT* `/api/<table_name>/<int>`
  - Updates existing record with primary key of the given int.
  - Data to update must be included in POST request as json
  - Returns JSON data in format `{success: <int>}` where <int> is the number
    of records successfully updated.

- *DELETE* `/api/<table_name>`
  - Deletes existing records. JSON data must be included in DELETE request in the
    format `{'rows': [obj1, obj2, ...]}`.
  - Each row must include a primary key column and value.
  - Returns JSON data in format `{success: <int>}` where <int> is the number
    of records successfully deleted.

- *DELETE* `/api/<table_name>/<int>`
  - Updates existing record with primary key of the given int.
  - Returns JSON data in format `{success: <int>}` where <int> is the number
    of records successfully deleted.
