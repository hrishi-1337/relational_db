import yaml
import os
from data.data_scripts.build_blocks import make_blocks
from data.data_scripts.make_index import make_index
with open('config/config.yaml') as f:
    cfg = yaml.safe_load(f)

root = cfg['rootdir'] = os.getcwd()+'/'
BLOCKSIZE = cfg['blocksize']

print("CONFIG")
print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
print(yaml.dump(cfg))
print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")

with open('config/config.yaml', 'w') as f:
    data = yaml.dump(cfg, f)

if cfg['build_blocks']['csv'] or cfg['build_blocks']['binary']:
    # make_blocks(root+'data/full/', BLOCKSIZE, csv=cfg['build_blocks']['csv'], binary=cfg['build_blocks']['binary'])
    make_blocks(root+'data/develop/', BLOCKSIZE, csv=cfg['build_blocks']['csv'], binary=cfg['build_blocks']['binary'])

for table,v in cfg['indexes'].items():
    for col in v:
        make_index('compact_'+table, col, root+'/data/develop/')
        # make_index(table, col, root+'/data/full/')
