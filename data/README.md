# Data directory
- **develop** will hold smaller dataset for development
- **full** has full dataset for experiments

- ```<dataset>/disk``` holds directories for each table
  - each table directory has a file for each block
- ```<dataset>/no_block_data``` holds csv files of all data


### Building Data
run ```build_blocks.py``` to build all blocks from csv files
