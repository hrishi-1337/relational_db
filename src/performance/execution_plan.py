from src.queries.nested_loop import NestedLoop
from src.queries.block_nested_loop import BlockNestedLoop
from src.queries.indexed_nested_loop import IndexedJoin
from src.queries.linear_scan import LinearScan
from src.performance.cost_estimation import CostEstimation
from definitions import data_version

from termcolor import colored


def execute_query(table, rows, where, where_clause):
    # table:        list of tables
    # join:         table to join on, False if no join
    # join_col:     column to join on (joins for equal column attributes), None if no join
    # where:        True if where condition exists, False if no where condition
    # where_clause: dictionary with the following keys
    #                       cols: colums names
    #                       ops: either {<,>,=} as strings
    #                       vals: value to compare data to
    #                  these are in a dict so there could be more than 1 where clause
    #                   for example: where NOC = 'ARG' Age > 30
    obj = CostEstimation()
    if len(table) > 1:
        print(colored('======= Estimated Costs =======', 'red'))
        print(colored("Outer relation: "+table[0]+", Inner relation: "+table[1]))
        cost1 = obj.cost(table)
        table.reverse()
        print(colored("Outer relation: " + table[0] + ", Inner relation: " + table[1]))
        cost2 = obj.cost(table)
        if cost1.iloc[0]['Nested Loop Join'] < cost2.iloc[0]['Nested Loop Join']:
            table.reverse()
        print("\n")
        print(colored("Optimum join relations :: Outer relation: " + table[0] + ", Inner relation: " + table[1], 'red'))
        print(colored('======= Nested Loop Join =======', 'red'))
        NestedLoop(data_version, table, rows, where, where_clause['cols'], where_clause['ops'], where_clause['vals'])
        print("\n")
        print(colored('======= Block-Nested Loop Join =======', 'red'))
        BlockNestedLoop(data_version, table, rows, where, where_clause['cols'], where_clause['ops'], where_clause['vals'])
    else:
        print(colored('======= Linear Scan =======', 'red'))
        LinearScan(data_version, table, rows, where, where_clause['cols'], where_clause['ops'], where_clause['vals'])


### ops
### we can pas
def lt(item1, item2):
    if item1<item2:
        return True
    return False

def  gt(item1, item2):
    if item1>item2:
        return True
    return False

def  eq(item1, item2):
    if item1==item2:
        return True
    return False
