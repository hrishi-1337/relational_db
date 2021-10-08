import pytest
from src.queries.nested_loop import NestedLoop

tables = ['compact_athletes', 'compact_athletic_events']
block_list = [19, 28]
obj = NestedLoop("develop", tables, True, "NOC", "=", "CHN")

def test_nested():
    assert obj.nested(block_list)['Name'].to_string(index=False) == 'A Dijiang'