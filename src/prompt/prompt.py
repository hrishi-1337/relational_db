""" implementation of query prompt """
import yaml
from sys import exit
import os
from termcolor import colored
from definitions import root
from src.performance.execution_plan import execute_query



def get_root():
    with open('config/config.yaml') as f:
        root = yaml.safe_load(f)['rootdir']
    return root


def print_help():
    root = get_root()
    with open(root + '/src/prompt/intro.txt', 'r') as f:
        for line in f:
            print(line, end="")
    print()


def print_tables():
    l = get_tables_list()
    print("tables")
    print("================")
    for i in l:
        print(i)
    print()

def print_premade():
    with open(root + '/src/prompt/premade.txt', 'r') as f:
        for line in f:
            if line[0] == 'Q':
                print(colored(line, 'yellow'), end="")
            else:
                print(line, end="")

def load_premade():
    p = []
    root = get_root()
    with open(root + '/src/prompt/premade.txt', 'r') as f:
        for line in f:
            if line[0].isdigit():
                p.append(line[:-1])
    return p

def run_preamde(inp):
    premade = load_premade()
    idx = int(inp)-1
    if idx < 0 or idx >= len(premade):
        print(colored('error:', 'red'), end=" ")
        print(f'there is not a pre-made query for number {inp}')
        return

    premade = [x[4:] for x in premade]
    parse_query(premade[idx])


def get_tables_list():
    root = get_root()
    tables = []
    tables = os.listdir(root + '/data/develop/disk/')
    tables += os.listdir(root + '/data/full/disk/')
    tables = list(dict.fromkeys(tables))
    for i in tables.copy():
        if i.endswith('bin'):
            tables.remove(i)
    return tables


def get_cols(tableL):
    root = get_root()
    cols = []
    for table in tableL:
        if table in os.listdir(root + '/data/develop/disk/'):
            with open(root + '/data/develop/disk/' + table + '/block0.csv', 'r') as f:
                for line in f:
                    cols += line[:-1].split(",")
                    break
        elif table in os.listdir(root + '/data/full/disk/'):
            with open(root + '/data/full/disk/' + table + '/block0.csv', 'r') as f:
                for line in f:
                    cols += line[:-1].split(",")
                    break
    if not cols:
        print(colored('error:', 'red'), end=" ")
        print(f"can't retrieve columns from {table}")
    return cols


def parse_query(q):
    ql = q.split(' ')
    where = False
    where_clause = {'cols': [], 'ops': [], 'vals': []}
    tables = get_tables_list()
    if 'from' not in ql:
        print(colored('error:', 'red'), end=" ")
        print('missing \'from\' in query')
        return
    elif ql.index('from') != 2:
        print(colored('error:', 'red'), end=" ")
        print('\'from\' should be third word in query')
        return
    elif len(ql) == 3:
        print(colored('error:', 'red'), end=" ")
        print('table is missing after \'from\'')
        return
    for table in ql[3].split(","):
        if table not in tables:
            print(colored('error:', 'red'), end=" ")
            print(f'the table {ql[3]} is not on disk')
            print_tables()
            return

    query_tables = ql[3].split(",")
    table_cols = get_cols(query_tables)
    rows = ql[1].split(",")
    if rows == ['*']:
        rows = table_cols

    for r in rows:
        if r not in table_cols:
            print(colored('error:', 'red'), end=" ")
            print(f'{r} is not a column')
            return
    if 'where' in ql:
        w = ql[5:]
        if len(w) % 3 != 0:
            print(colored('error:', 'red'), end=" ")
            print("where clause needs to be three words: <col> <op> <val>")
            return
        elif not len(w):
            print(colored('error:', 'red'), end=" ")
            print("where clause is empty")
            return
        for i in range(0, len(w), 3):
            where_clause['cols'].append(w[i])
            where_clause['ops'].append(w[i + 1])
            where_clause['vals'].append(w[i + 2])
        for i in where_clause['ops']:
            if i not in ['<', '>', '=']:
                print(colored('error:', 'red'), end=" ")
                print(f"{i} should be any of <,>,=")
                return
        where = True

    poss = ['select', 'from', 'join', 'on', 'where', ",".join(query_tables), ",".join(rows)] + where_clause['cols'] + \
           where_clause['ops'] + where_clause['vals'] +["*"]
    for w in ql:
        if w not in poss:
            print(colored('error:', 'red'), end=" ")
            print(f"{w} is unknown")
            return

    print(colored('query passes parsing', 'green'))
    execute_query(table=query_tables, rows=rows, where=where, where_clause=where_clause)


def show_format():
    print(colored('select', 'green'), end=" ")
    print("column", end=" ")
    print(colored('from', 'green'), end=" ")
    print("table", end=" ")
    print(colored('join', 'green'), end=" ")
    print("table", end=" ")
    print(colored('on', 'green'), end=" ")
    print("column", end=" ")
    print(colored('where', 'green'), end=" ")
    print("column", end=" ")
    print("{<,>,=}", end=" ")
    print("value", end=" ")
    print()


def prompt():
    print_help()
    while True:
        inp = input('> ')

        if inp == 'exit':
            break
        if inp.isdigit():
            run_preamde(inp)
        elif inp == '':
            continue
        elif inp == 'tables':
            print_tables()
        elif inp == 'pre-made':
            print_premade()
        elif inp == 'format':
            show_format()

        elif inp == 'help':
            print_help()

        elif inp.startswith('select '):
            parse_query(inp)

        else:
            print(colored('error:', 'red'), end=" ")
            print("Query must start with \'select \' or be in menu")
