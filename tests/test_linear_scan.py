import pytest
import os
from src.queries.linear_scan import LinearScan
from definitions import root

table = 'compact_athletes'
table_path = os.path.join(root, 'data', 'develop', 'disk', table)
obj = LinearScan()

def test_nested():
    assert obj.linear_scan(table_path, 19, "CHN").iloc[0]['Name'] == 'A Dijiang'