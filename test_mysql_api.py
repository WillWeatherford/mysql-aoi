"""Test module for MySQL API."""

import pytest
import test_config
from mysql import connector


@pytest.fixture
def create_tables(request):
    """Create tables for testing."""
    conn = connector.connect(**test_config.TEST_CONFIG)
    cursor = conn.cursor()
    cursor.execute(test_config.TABLES['company'])

    def teardown():
        cursor.execute('DROP TABLE `company`')
        cursor.close()
        conn.close()

    request.addfinalizer(teardown)


def test_simple(create_tables):
    """."""
    assert True
