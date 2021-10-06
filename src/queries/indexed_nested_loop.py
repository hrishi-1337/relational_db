""" indexed nested loop """
import pandas as pd
import os

class IndexedJoin:
    def __init__(self, data_version, table1, table2, join_col, where=False, where_col=None, where_op=None, where_val = None):
        # data_version: full or develop
        # table 1:      first table
        # table 2:      second table to join on
        # join_col:     column to join on (joins for equal column attributes)
        # where:        column for where condition, False if no where condition
        # where_op:     operator for where condtion(<,>,=), None if no where condition
        # where_val:    value for where condition, None if no where condition


        # if index join was called, the index must exist
        assert join_col in os.listdir(os.join('data',data_version,'indexes',join_col+'.csv'))
