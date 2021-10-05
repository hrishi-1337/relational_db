""" implementation of query prompt """
import yaml
from sys import exit
import os
from termcolor import colored

def send_query(table, join, join_col, where, where_clause):
    pass
def get_root():
    with open('config/config.yaml') as f:
        root = yaml.load(f)['rootdir']
    return root
def print_help():
    root = get_root()
    with open(root+'/src/prompt/intro.txt', 'r') as f:
        for line in f:
            print(line,end="")
    print()

def print_tables():
    l = get_tables_list()
    print("tables")
    print("================")
    for i in l:
        print(i)
    print()

def load_premade():
    p = []
    root = get_root()
    with open(root + '/src/prompt/premade.txt', 'r') as f:
        for line in f:
            p.append(line[:-1])
    return p

def get_tables_list():
    root = get_root()
    tables = []
    tables = os.listdir(root+'/data/develop/disk/')
    tables += os.listdir(root+'/data/full/disk/')
    tables = list(dict.fromkeys(tables))
    for i in tables.copy():
        if i.endswith('bin'):
            tables.remove(i)
    return tables

def get_cols(tableL):
    root = get_root()
    cols = []
    for table in tableL:
        if table in os.listdir(root+'/data/develop/disk/'):
            with open(root+'/data/develop/disk/'+table+'/block0.csv', 'r') as f:
                for line in f:
                    cols += line[:-1].split(",")
                    break
        elif table in os.listdir(root+'/data/full/disk/'):
            with open(root+'/data/full/disk/'+table+'/block0.csv', 'r') as f:
                for line in f:
                    cols += line[:-1].split(",")
                    break
    if not cols:
        print(colored('error:', 'red'),end=" ")
        print(f"can't retrieve columns from {table}")
    cols = [x.lower() for x in cols]
    return cols



def parse_query(q):
    ql = q.split(' ')
    table = None
    join = False
    join_col = None
    where = None
    where_clause = {'cols':[],'ops':[], 'vals':[]}
    tables = get_tables_list()
    if 'from' not in ql:
        print(colored('error:', 'red'),end=" ")
        print('missing \'from\' in query')
        return
    elif ql.index('from') != 2:
        print(colored('error:', 'red'),end=" ")
        print('\'from\' should be third word in query')
        return
    elif len(ql)==3:
        print(colored('error:', 'red'),end=" ")
        print('table is missing after \'from\'')
        return
    elif ql[3] not in tables:
        print(colored('error:', 'red'),end=" ")
        print(f'the table {ql[3]} is not on disk')
        print_tables()
        return
    table = ql[3]
    rows = ql[1].split(",")
    table_cols = get_cols([table])+['*']

    if 'join' in ql:
        idx = ql.index('join')
        if idx != 4:
            print(colored('error:', 'red'),end=" ")
            print('\'join\' needs to be the the 5th word in the query')
            return
        elif len(ql)==5:
            print(colored('error:', 'red'),end=" ")
            print('table is missing after \'join\'')
            return
        elif ql[5] not in tables:
            print(colored('error:', 'red'),end=" ")
            print(f'the table \'{ql[5]}\' to join on is not on disk')
            print_tables()
            return
        elif len(ql)==6:
            print(colored('error:', 'red'),end=" ")
            print(f'need col to join on')
            return
        elif ql[6] != 'on':
            print(colored('error:', 'red'),end=" ")
            print(f'7th words needs to be \'on\'')
            return
        elif len(ql)==7:
            print(colored('error:', 'red'),end=" ")
            print(f'need col to join on')
            return
        elif ql[6] != 'on':
            print(colored('error:', 'red'),end=" ")
            print(f'7th words needs to be \'on\'')
            return
        join_col = ql[7].replace("_", " ").replace("-", " ")
        if join_col not in table_cols:
            print(colored('error:', 'red'),end=" ")
            print(f'join column {join_col} needs to be in both tables')
            print("ROWS:")
            for i in join_table_cols:
                print(i)
            return

        join = ql[5]
        join_table_cols = get_cols([join])
        if join_col not in join_table_cols:
            print(colored('error:', 'red'),end=" ")
            print(f'join column {join_col} needs to be in both tables')
            print("ROWS:")
            for i in join_table_cols:
                print(i)
            return
        table_cols += join_table_cols
    for r in rows:
        if r not in table_cols:
            print(colored('error:', 'red'),end=" ")
            print(f'{r} is not a column')
            return
    if 'where' in ql:
        idx = ql.index('where')
        if join and idx != 6:
            print(colored('error:', 'red'),end=" ")
            print('\'where\' need to be seventh word if there is a join')
            return
        elif not join and idx != 4:
            print(colored('error:', 'red'),end=" ")
            print('\'where\' need to be fifth word if there is not a join')
            return
        else:
            if join:
                w = ql[9:]
            else:
                w = ql[5:]
            if len(w)%3!=0:
                print(colored('error:', 'red'),end=" ")
                print("where clause needs to be three words: <col> <op> <val>")
                return
            elif not len(w):
                print(colored('error:', 'red'),end=" ")
                print("where clause is empty")
                return
            for i in range(0,len(w),3):
                where_clause['cols'].append(w[i])
                where_clause['ops'].append(w[i+1])
                where_clause['vals'].append(w[i+2])
            for i in where_clause['ops']:
                if i not in ['<','>','=']:
                    print(colored('error:', 'red'),end=" ")
                    print(f"{i} should be any of <,>,=")
                    return

            # for i in where_clause['cols']:
            #     if i not in ['<','>','=']:
            #         print(colored('error:', 'red'),end=" ")
            #         print(f"{i} should be any of <,>,=")
            #         return
            where=True

    poss = ['sel', 'from', 'join', 'on','where', join, join_col, table] + rows + where_clause['cols'] + where_clause['ops'] + where_clause['vals']
    for w in ql:
        if w not in poss:
            print(colored('error:', 'red'),end=" ")
            print(f"{w} is unknown")
            return

    print(colored('query passes parsing', 'green'))
    send_query(table=table, join=join, join_col=join_col, where=where, where_clause=where_clause)


def show_format():
    print(colored('sel', 'green'),end=" ")
    print("column", end = " ")
    print(colored('from', 'green'),end=" ")
    print("table", end = " ")
    print(colored('join', 'green'),end=" ")
    print("table", end = " ")
    print(colored('on', 'green'),end=" ")
    print("column", end = " ")
    print(colored('where', 'green'),end=" ")
    print("column", end = " ")
    print("{<,>,=}", end = " ")
    print("value", end = " ")
    print()

def prompt():
    print_help()
    while True:
        inp = input('> ').lower()

        if inp == 'exit':
            break
        elif inp == '':
            continue
        elif inp == 'tables':
            print_tables()
        elif inp == 'pre-made':
            for i in load_premade():
                print(i)
        elif inp == 'format':
            show_format()

        elif inp == 'help':
            print_help()

        elif inp.startswith('sel '):
            parse_query(inp)
        else:
            print(colored('error:', 'red'),end=" ")
            print("Query must start with \'sel \' or be in menu")