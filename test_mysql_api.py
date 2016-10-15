"""Test module for MySQL API."""

import pytest
import test_config
from mysql import connector


@pytest.fixture
def create_tables():
    """Create tables for testing."""
    conn = connector.connect(**test_config.TEST_CONFIG)
    cursor = conn.cursor()
    cursor.execute(test_config.TABLES['company'])
    cursor.close()
    conn.close()

