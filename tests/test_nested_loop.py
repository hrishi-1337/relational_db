import pytest
from src.queries.nested_loop import NestedLoop

DB_PATH = '../data/develop/disk/'
tables = ['compact_athletes', 'compact_athletic_events']
obj = NestedLoop()

def test_nested():
    assert obj.tables(DB_PATH, {}, tables, "CHN")['Name'].to_string(index=False) == 'A Dijiang'