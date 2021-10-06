import os, yaml

from data.data_scripts.build_blocks import make_blocks
from data.data_scripts.make_index import make_index

root = os.path.dirname(os.path.abspath(__file__))
with open(root+'/config/config.yaml') as f:
    cfg = yaml.safe_load(f)
BLOCKSIZE = cfg['blocksize']

make_blocks(root+'/data/develop/', BLOCKSIZE, csv=True, binary=True)

# for table,v in cfg['indexes'].items():
#     for col in v:
#         make_index('compact_'+table, col, root+'/data/develop/')
#         # make_index(table, col, root+'/data/full/')



from src.queries.indexed_nested_loop import IndexedJoin

j = IndexedJoin()
