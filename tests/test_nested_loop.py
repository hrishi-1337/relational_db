import pytest
import os
from src.queries.nested_loop import NestedLoop
from definitions import root

DB_PATH = os.path.join(root, 'data', 'develop', 'disk')
tables = ['compact_athletes', 'compact_athletic_events']
block_list = [19, 28]
obj = NestedLoop()

def test_nested():
    assert obj.nested(DB_PATH, tables, block_list, "CHN")['Name'].to_string(index=False) == 'A Dijiang'