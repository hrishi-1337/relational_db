import pytest
from src.queries.block_nested_loop import BlockNestedLoop

tables = ['compact_athletes', 'compact_athletic_events']
block_list = [19, 28]
obj = BlockNestedLoop("develop", tables, True, "NOC", "=", "CHN")

def test_nested():
    assert obj.block_nested(block_list)['Name'].to_string(index=False) == 'A Dijiang'