import logging
import pytest
from unittest.mock import patch, MagicMock

logger = logging.getLogger(__name__)

@pytest.fixture(scope='session')
def mock_bigquery_client():
    mock_client = MagicMock()
    with patch('google.cloud.bigquery.Client', autospec=False):
        yield mock_client

@pytest.fixture(scope='session')
def client(mock_bigquery_client):
    from freg_quality_metrics.app import app as api
    client = api.test_client()
    return client

# from api import bigquery


# from app.app import app as api
# client = api.test_client()

def test_ready(client):
    """Tests the ready endpoint. Is always 200 for now"""
    response = client.get("/health/ready")
    assert response.status_code == 200
    assert response.text == "Ready!"


def test_alive(client):
    """Tests the alive endpoint. Is always 200 for now"""
    response = client.get("/health/alive")
    assert response.status_code == 200
