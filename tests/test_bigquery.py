from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(scope="module")
def bq(bigquery_client):
    from freg_quality_metrics.bigquery import BigQuery

    return BigQuery()


""" Commented out since check is no longer done in python but in bigquery

@pytest.mark.parametrize("test_input,expected", [
    ("2222",(False,"format")),
    ("01234567891",(False,"date")),
    ("01012000a1",(False,"format")),
    ("010120002398573984753241",(False,"format")),
])
def test_valid_fnr_or_dnr(test_input,expected,BQ):
    assert BQ._valid_fnr_or_dnr(test_input) == expected
"""
