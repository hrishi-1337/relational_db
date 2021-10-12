import os, yaml

from data.data_scripts.build_blocks import make_blocks
from data.data_scripts.make_index import make_index

root = os.path.dirname(os.path.abspath(__file__))
with open(root+'/config/config.yaml') as f:
    cfg = yaml.safe_load(f)
BLOCKSIZE = cfg['blocksize']

# make_blocks(root+'/data/develop/', BLOCKSIZE, csv=True, binary=True)

# make_index('compact_athletes', 'NOC', root+'/data/develop/')
# make_index('noc_regions', 'NOC', root+'/data/develop/')
# make_index('compact_athletes', 'Athlete ID', root+'/data/develop/')
# make_index('compact_athletic_events', 'Athlete ID', root+'/data/develop/')
# make_index(table, col, root+'/data/full/')

# make_index('athletes', 'NOC', root+'/data/full/')

# from src.queries.indexed_nested_loop import IndexedJoin
#
# j = IndexedJoin(data_version='develop', table1='noc_regions', table2='compact_athletic_events', join_col='NOC')
# data = j.join()
# print(data)

from src.b_tree.b_tree import BTree
index_path = 'data/full/indexes/athletes_NOC'
tree = BTree(index_path, 'NOC')
tree.horizontal_print()
