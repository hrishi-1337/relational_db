import pytest
from src.queries.nested_loop import NestedLoop

tables = ['compact_athletes', 'compact_athletic_events']
columns = {'compact_athletes': ['Athlete ID', 'Name', 'Sex', 'Age', 'Height', 'Weight', 'Team', 'NOC'],
           'compact_athletic_events': ['Athlete ID', 'NOC', 'Games', 'Year', 'Season', 'City', 'Sport', 'Event', 'Medal']}
block_list = [19, 28]
obj = NestedLoop("develop", tables, ['Name'], True, ["NOC"], ["="], ["CHN"])

def test_nested():
    assert obj.nested(block_list, columns)['Name'].to_string(index=False) == 'A Dijiang'