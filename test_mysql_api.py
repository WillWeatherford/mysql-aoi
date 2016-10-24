"""Test module for MySQL API."""

import os
import json
import pytest
import requests
import test_config
from mysql import connector

API_URL = 'http://127.0.0.1:5000/api'


TEST_RECORD = {
    'entity_id': '999999999',
    'co_name': 'TestCorp',
    'pbid': '999999999',
    'weburl': 'www.test-test-test.biz',
}
TEST_RECORD_PATH = '/'.join((API_URL, 'company', TEST_RECORD['entity_id']))


@pytest.fixture(scope='session')
def app():
    """Set the config module value in the os environment to test."""
    os.environ['MYSQL_CONFIG_MODULE'] = 'test_config'
    import mysql_api
    app = mysql_api.app.test_client()
    return app


@pytest.fixture
def create_tables(request):
    """Create tables for testing."""
    conn = connector.connect(**test_config.CONNECT_PARAMS)
    cursor = conn.cursor()
    cursor.execute(test_config.TABLES['company'])

    def teardown():
        cursor.execute('DROP TABLE `company`')
        cursor.close()
        conn.close()

    request.addfinalizer(teardown)


# def test_get_all(create_tables, app):
#     """Test that a get request successfully gets stuff."""
#     url = '/'.join((API_URL, 'company'))
#     response = app.get(url)
#     import pdb;pdb.set_trace()
#     assert 'rows' in response.json()


# def test_post_one(create_tables, app):
#     """Test that new data can be posted."""
#     data = {'rows': [{
#         'entity_id': 1,
#         'weburl': 'www.site.com',
#         'co_name': 'TestCo',
#         'pbid': '1',
#     }]}
#     url = '/'.join((API_URL, 'company'))
#     response = app.post(url, data=json.dumps(data))

#     assert response.status_code == 200
#     # assert response.json() == {'success': 1}


# SYSTEM TESTS ON RUNNING SERVER

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


def test_get_one():
    """Test getting one record from the real database."""
    resp = requests.get('/'.join((API_URL, 'company', '1')))
    assert resp.status_code == 200
    assert resp.json().get('entity_id') == '1'


# def test_get_one_hundred():
#     """Test getting one record from the real database."""
#     resp = requests.get('/'.join((API_URL, 'company')), params={'num_rows': 100})
#     assert resp.status_code == 200
#     assert len(resp.json().get('rows')) == 100


def test_one_posted_status(one_posted):
    """Test that posting one to real DB results in 200 status code."""
    assert one_posted.status_code == 200


def test_one_posted_success(one_posted):
    """Test that posting one to real DB gives success response."""
    assert one_posted.json().get('success') == 1


def test_one_posted_get_status(one_posted):
    """Test that posting one to real DB gives success response."""
    get_resp = requests.get(TEST_RECORD_PATH)
    assert get_resp.status_code == 200


def test_one_posted_get_data(one_posted):
    """Test that posting one to real DB gives success response."""
    get_resp = requests.get(TEST_RECORD_PATH)
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
    assert delete_resp.json().get('success') == 1
