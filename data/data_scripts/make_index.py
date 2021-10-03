#!/usr/local/bin/python3

""" files to make index on a table """
import pandas as pd
from sys import exit
import os, sys


def make_index(table, column, dir):
    print("making index for table:", table, 'column:', column+'...')
    index_df = pd.DataFrame(columns = ['ptr', column])                      # make empty index dataframe
    for blockfile in os.listdir(dir+'disk/'+table):                             # loop through all block files
        if blockfile[-4:] =='.csv':
            block_num = blockfile[5:-4]                                         # get blobk number based on filename
            df = pd.read_csv(dir+'disk/'+table+'/'+blockfile)                       # read block in
            df['ptr'] = [block_num+'-'+str(i) for i in range(len(df.index))]    # add pointer column to dataframe
            df = df[['ptr',column]]                                             # reduce to dataframe to ptr and key
            index_df = index_df.append(df)                                      # append to main index df

    # sort index and write to csv file
    print('sorting...')
    index_df = index_df.sort_values("Team")
    index_df.to_csv(dir+'indexes/'+column+'.csv',index=False)
    pfile = dir+'indexes/'+column+'.p'
    index_df.to_pickle(pfile)
    print("Made index.")


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
