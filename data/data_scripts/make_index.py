#!/usr/local/bin/python3

""" files to make index on a table """
import pandas as pd
from sys import exit
import os, sys
import pickle
# import resource

from src.b_tree.b_tree import BTree


def make_index(table, column, dir):
    cname = column.replace(" ", "_")
    column = column.replace("_", " ")
    print("making index for table:", table, 'column:', column+'...')
    index_df = pd.DataFrame(columns = ['ptr', column])                          # make empty index dataframe
    for blockfile in os.listdir(dir+'disk/'+table):                             # loop through all block files
        if blockfile[-4:] =='.csv':
            block_num = blockfile[5:-4]                                         # get block number based on filename
            df = pd.read_csv(dir+'disk/'+table+'/'+blockfile)                   # read block in
            df['ptr'] = [block_num+'-'+str(i) for i in range(len(df.index))]    # add pointer column to dataframe
            df = df[['ptr',column]]                                             # reduce to dataframe to ptr and key
            index_df = index_df.append(df)                                      # append to main index df

    # sort index and write to csv file
    print('sorting...')
    index_df = index_df.sort_values(column)
    index_df.to_csv(dir+'indexes/'+table+'_'+cname+'.csv',index=False)
    pfile = dir+'indexes/'+table+'_'+cname+'.p'
    index_df.to_pickle(pfile)
    print("Made index.")

    # if 'full' in dir:
    #     sys.setrecursionlimit(1000000)
    #     resource.setrlimit(resource.RLIMIT_STACK, [0x100 * 1000000, resource.RLIM_INFINITY])
    #     print('saving b-tree as binary file...')
    #     if not os.path.exists('data/full/saved_btrees'):
    #         os.mkdir('data/full/saved_btrees')
    #     idx = dir+'indexes/'+table+'_'+cname
    #     btree = BTree(idx, column)
    #     with open('data/full/saved_btrees/'+cname+'.p', 'wb') as f:
    #         pickle.dump(btree, f)
    #     print('btree saved.')




if __name__ == "__main__":
    args = sys.argv
    assert len(args) >= 4, "Need at least three args: Directory, Table, and Column(s)"
    dir = args[1]
    assert dir in ['develop','full'], "first argument needs to be 'develop' or 'full'"
    table = args[2]
    tables = os.listdir('../'+dir+'/disk')
    assert table in tables, f"{table} not in tables dirs: {tables}"
    cols = args[3:]
    for col in cols:
        make_index(table, col, dir)
