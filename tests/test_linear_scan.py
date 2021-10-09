import pytest
from src.queries.linear_scan import LinearScan

obj = LinearScan("develop", ['compact_athletes'], ['Name'], True, ["NOC"], ["="], ["CHN"])

def test_nested():
    assert obj.linear_scan( 19).iloc[0]['Name'] == 'A Dijiang'