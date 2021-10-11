import os
from sys import exit
import time
import pandas as pd
import pickle

from definitions import root, BLOCKSIZE, BINARY
from src.b_tree.b_tree import BTree


class IndexedScan:
    def __init__(self, data_version, table, rows, where, where_col, where_op, where_val):
        self.data_version = data_version
        self.rows = rows
        self.where = where
        self.where_col = [i.replace("_", " ") for i in where_col]
        self.where_op = where_op
        self.where_val = [i.replace("_", " ") for i in where_val]
        if self.where_val[0].isdigit():
            self.where_val = [int(i) for i in where_val]
        self.table_path = os.path.join(root, 'data', self.data_version, 'disk', table[0])

        # metrics
        self.seeks = 1
        self.block_reads = 0

        # load b-tree
        print('loading b-tree...')
        idx_path = os.path.join('data', data_version, 'indexes', table[0]+'_'+where_col[0].replace(" ","_"))
        col = where_col[0].replace("_", " ")
        if False:#data_version == 'full':
            with open('data/full/saved_btrees/'+col+'.p', 'rb') as f:
                btree = pickle.load(f)
        else:
            btree = BTree(idx_path, col)
        print(btree.get_depth(btree.root))
        print('b-tree loaded.')


        # get starting node
        start_node  = btree.get_start_node(btree.root, self.where_val[0])
        print(start_node.key)


        block_count = int(len(os.listdir(self.table_path)))
        result = self.index_scan(start_node)
        print("Result: ")
        try:
            print(result[self.rows])
        except Exception as e:
            print("No results found")

    def index_scan(self, start_node):
        start_time = time.time()
        block_num = int(start_node.ptr.split("-")[0])
        result = pd.DataFrame()
        while True:
            if BINARY:
                block = pd.read_pickle(self.table_path+'_bin'+ "/block" + str(block_num) + ".p")
            else:
                block = pd.read_csv(self.table_path + "/block" + str(i) + ".csv")
            self.block_reads += 1
            if block.iloc[0][self.where_col[0]] > self.where_val[0]:
                break
            for j in range(BLOCKSIZE-1):
                    if block.iloc[j][self.where_col[0]] == self.where_val[0]:
                        result = result.append(block.iloc[j], ignore_index=True)
            block_num +=1
        run_time = time.time() - start_time
        print("Runtime: "+"%.3f" % run_time+ " Seconds")
        print("Block Transfers: " +str(self.block_reads))
        print("Seeks: ", self.seeks)
        return result
