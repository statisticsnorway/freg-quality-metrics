"""
Fixtures that will be used by multiple test files should be placed here
"""

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="session")
def bigquery_client():
    mock_client = MagicMock()
    with patch("google.cloud.bigquery.Client", autospec=False):
        yield mock_client
