import logging
from fastapi.testclient import TestClient
from app.main import app as api

client = TestClient(api)

logger = logging.getLogger()


def test_ready():
    """Tests the ready endpoint. Is always 200 for now"""
    response = client.get("/health/ready")
    assert response.status_code == 200
    assert response.json() == "Ready!"


def test_alive():
    """Tests the alive endpoint. Is always 200 for now"""
    response = client.get("/health/alive")

    assert response.status_code == 200
