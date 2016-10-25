"""Test module for MySQL API."""

import os
import json
import pytest
import requests
import config_test
from copy import deepcopy
from mysql import connector
from operator import itemgetter

API_URL = 'http://127.0.0.1:5000/api'


TEST_RECORD = {
    'entity_id': '999999999',
    'co_name': 'TestCorp',
    'pbid': '999999999',
    'weburl': 'www.test-test-test.biz',
}
TEST_RECORD_PATH = '/'.join((API_URL, 'company', TEST_RECORD['entity_id']))

NUM_TEST_RECORDS = 12

TEST_RECORDS = []
for n in range(NUM_TEST_RECORDS):
    record = TEST_RECORD.copy()
    for key, val in record.items():
        record[key] = val + str(n)
    record['weburl'] = 'www.filterbythisurl.com'
    TEST_RECORDS.append(record)


@pytest.fixture(scope='session')
def app():
    """Set the config module value in the os environment to test."""
    os.environ['MYSQL_CONFIG_MODULE'] = 'config_test'
    import mysql_api
    app = mysql_api.app.test_client()
    return app


@pytest.fixture
def create_tables(request):
    """Create tables for testing."""
    conn = connector.connect(**config_test.CONNECT_PARAMS)
    cursor = conn.cursor()
    cursor.execute(config_test.TABLES['company'])

    def teardown():
        cursor.execute('DROP TABLE `company`')
        cursor.close()
        conn.close()

    request.addfinalizer(teardown)


# SYSTEM TESTS ON RUNNING SERVER


def test_get_one():
    """Test getting one record from the real database."""
    resp = requests.get('/'.join((API_URL, 'company', '1')))
    assert resp.status_code == 200
    assert resp.json().get('entity_id') == '1'


def test_get_many():
    """Test getting one record from the real database."""
    resp = requests.get(
        '/'.join((API_URL, 'company')),
        json={'num_rows': NUM_TEST_RECORDS}
    )
    assert resp.status_code == 200
    assert len(resp.json().get('rows')) == NUM_TEST_RECORDS


################################
# Test against one row posted.

@pytest.fixture
def one_posted(request):
    """Set up by posting a new record to the actual live database."""
    response = requests.post(
        '/'.join((API_URL, 'company')),
        json={'rows': [TEST_RECORD]}
    )

    def delete_one():
        requests.delete(TEST_RECORD_PATH)

    request.addfinalizer(delete_one)
    return response


def test_one_posted_status(one_posted):
    """Test that posting one to real DB results in 200 status code."""
    assert one_posted.status_code == 200


def test_one_posted_success(one_posted):
    """Test that posting one to real DB gives success response."""
    assert one_posted.json().get('success') == 1


def test_one_posted_get(one_posted):
    """Test that posting one to real DB gives success response."""
    get_resp = requests.get(TEST_RECORD_PATH)
    assert get_resp.status_code == 200
    assert get_resp.json() == TEST_RECORD


def test_one_posted_update(one_posted):
    """Test that posting one to real DB gives success response."""
    update_resp = requests.put(
        TEST_RECORD_PATH,
        json={'weburl': 'www.better-test-company.biz'},
    )
    assert update_resp.status_code == 200
    assert update_resp.json().get('success') == 1


def test_one_posted_delete(one_posted):
    """Test that posting one to real DB gives success response."""
    delete_resp = requests.delete(TEST_RECORD_PATH)
    assert delete_resp.status_code == 200
    assert delete_resp.json().get('success') == 1


################################
# Test against many rows posted.

@pytest.fixture
def many_posted(request):
    """Set up by posting a new record to the actual live database."""
    response = requests.post(
        '/'.join((API_URL, 'company')),
        json={'rows': TEST_RECORDS}
    )

    def delete_many():
        requests.delete(
            '/'.join((API_URL, 'company')),
            json={'rows': TEST_RECORDS}
        )

    request.addfinalizer(delete_many)
    return response


def test_many_posted_status(many_posted):
    """Test that posting one to real DB results in 200 status code."""
    assert many_posted.status_code == 200


def test_many_posted_success(many_posted):
    """Test that posting one to real DB gives success response."""
    assert many_posted.json().get('success') == NUM_TEST_RECORDS


def test_many_posted_get(many_posted):
    """Test that posting one to real DB gives success response."""
    path = '/'.join((API_URL, 'company'))
    criteria = {'weburl': 'www.filterbythisurl.com'}
    get_resp = requests.get(path, json={'criteria': criteria})
    assert get_resp.status_code == 200

    results = get_resp.json()['rows']
    key = itemgetter('entity_id')
    assert sorted(results, key=key) == sorted(TEST_RECORDS, key=key)


def test_many_posted_update(many_posted):
    """Test that posting one to real DB gives success response."""
    records = deepcopy(TEST_RECORDS)
    for record in records:
        record['weburl'] = 'www.updated.com'
    update_resp = requests.put(
        '/'.join((API_URL, 'company')),
        json={'rows': records},
    )
    assert update_resp.status_code == 200
    assert update_resp.json().get('success') == NUM_TEST_RECORDS


def test_many_posted_delete(many_posted):
    """Test that posting one to real DB gives success response."""
    delete_resp = requests.delete(
        '/'.join((API_URL, 'company')),
        json={'rows': TEST_RECORDS}
    )
    assert delete_resp.status_code == 200
    assert delete_resp.json().get('success') == NUM_TEST_RECORDS


###########################
# Fail case tests

@pytest.mark.parametrize('method', ('GET', 'PUT', 'DELETE'))
def test_bad_pk_404(method):
    """Test that a 404 response is returned when a bad PK path is given."""
    pk = "definitelynotarealpk"
    method_func = getattr(requests, method.lower())
    path = '/'.join((API_URL, 'company', pk))
    response = method_func(path, json={})
    assert response.status_code == 404
