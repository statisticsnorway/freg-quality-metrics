import logging
import pytest
from google.cloud import bigquery

logger = logging.getLogger()

# @pytest.fixture(scope='function')
# def mock_bigquery_client(monkeypatch):
#     def mockclient(*args, **kwargs):
#         return None
#     monkeypatch.setattr(bigquery,"Client",mockclient)

# @pytest.fixture(scope='function')
# def client(mock_bigquery_client):
#     from app.app import app as api
#     client = api.test_client()
#     return client

from app.app import app as api
client = api.test_client()

def test_ready():
    """Tests the ready endpoint. Is always 200 for now"""
    response = client.get("/health/ready")
    assert response.status_code == 200
    assert response.text == "Ready!"


def test_alive():
    """Tests the alive endpoint. Is always 200 for now"""
    response = client.get("/health/alive")

    assert response.status_code == 200
