from data.data_scripts.build_blocks import make_blocks
from data.data_scripts.make_index import make_index
from definitions import *


# if cfg['build_blocks']['csv'] or cfg['build_blocks']['binary']:
#     make_blocks(root+'/data/full/', BLOCKSIZE, csv=cfg['build_blocks']['csv'], binary=cfg['build_blocks']['binary'])
    # make_blocks(root+'/data/develop/', BLOCKSIZE, csv=cfg['build_blocks']['csv'], binary=cfg['build_blocks']['binary'])

for table,v in cfg['indexes'].items():
    for col in v:
#         # make_index('compact_'+table, col, root+'/data/develop/')
        make_index(table, col, root+'/data/full/')
