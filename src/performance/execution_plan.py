


def execute_query(table=table, join=join, join_col=join_col, where=where, where_clause=where_clause):
    # table:        first table
    # join:         table to join on, False if no join
    # join_col:     column to join on (joins for equal column attributes), None if no join
    # where:        True if where condition exists, False if no where condition
    # where_clause: dictionary with the following keys
    #                       cols: colums names
    #                       ops: either {<,>,=} as strings
    #                       vals: value to compare data to
    #                  these are in a dict so there could be more than 1 where clause
    #                   for example: where NOC = 'ARG' Age > 30

    # todo 
    pass



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
