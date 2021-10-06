import pytest
import os
from src.queries.block_nested_loop import BlockNestedLoop
from definitions import root

DB_PATH = os.path.join(root, 'data', 'develop', 'disk')
tables = ['compact_athletes', 'compact_athletic_events']
block_list = [19, 28]
obj = BlockNestedLoop()

def test_block_nested():
    assert obj.block_nested(DB_PATH, tables, block_list, "CHN")['Name'].to_string(index=False) == 'A Dijiang'