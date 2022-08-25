import pytest
from unittest.mock import patch, MagicMock

from freg_quality_metrics.api.bigquery import BigQuery

@pytest.fixture(scope='module')
def BQ(bigquery_client):
    from freg_quality_metrics.api.bigquery import BigQuery
    BQ = BigQuery()
    return BQ


@pytest.mark.parametrize("test_input,expected", [
    ("2222",(False,"format")),
    ("01234567891",(False,"date")),
    ("01012000a1",(False,"format")),
    ("010120002398573984753241",(False,"format")),
    # ("2222",(False,"format")),
    # ("2222",(False,"format")),
    # ("2222",(False,"format")),
])
def test_valid_fnr_or_dnr(test_input,expected,BQ: BigQuery):
    assert BQ._valid_fnr_or_dnr(test_input) == expected
