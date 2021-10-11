import yaml
import os

root = os.path.dirname(os.path.abspath(__file__))
with open(root+'/config/config.yaml') as f:
    cfg = yaml.safe_load(f)
BLOCKSIZE = cfg['blocksize']
BINARY = cfg['binary']
data_version = cfg['data_version']
cfg['rootdir'] = root
print("CONFIG")
print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%\n")
print(yaml.dump(cfg))
print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")

with open(root+'/config/config.yaml', 'w') as f:
    data = yaml.dump(cfg, f)
