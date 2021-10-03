# Data directory
- **develop** will hold smaller dataset for development
- **full** has full dataset for experiments

- ```<dataset>/disk``` holds directories for each table
  - each table directory has a file for each block (csv and binary)
- ```<dataset>/no_block_data``` holds csv files of all data
- ```<dataset>/indexes``` holds index files (binary and csv)

### data_scripts
directory holds scripts for data:
- ```build_blocks.py```
- ```make_index.py```

### Building Data
To save storage on github we don't upload indexes or data blocks. Run ```start.py``` to call data building scripts
