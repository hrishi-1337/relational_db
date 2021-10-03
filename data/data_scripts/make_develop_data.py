import shutil, os
import pandas as pd

# make dataset this many times smaller
DIV = 1000


shutil.rmtree('./develop')
os.mkdir('develop')
os.mkdir('develop/no_block_data')
os.mkdir('develop/indexes')
os.mkdir('develop/disk')



data_path = 'full/no_block_data/'
for csv_file in os.listdir(data_path):
    print("making compact", csv_file, "in develop")
    if csv_file == 'noc_regions.csv':
        shutil.copyfile(data_path + csv_file, 'develop/no_block_data/' + csv_file)
    else:
        df = pd.read_csv(data_path+csv_file)
        ilen = len(df.index)
        df = df.drop([x for x in range(ilen) if x%DIV!=0])
        print(df)
        df.to_csv('develop/no_block_data/compact_'+csv_file)
