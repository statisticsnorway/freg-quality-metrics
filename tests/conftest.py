"""
Fixtures that will be used by multiple test files should be placed here
"""

import pytest
from unittest.mock import patch, MagicMock

@pytest.fixture(scope='session')
def bigquery_client():
    mock_client = MagicMock()
    with patch('google.cloud.bigquery.Client', autospec=False):
        yield mock_client