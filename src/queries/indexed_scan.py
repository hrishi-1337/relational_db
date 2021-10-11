import os
from sys import exit
import time
import pandas as pd
import pickle

from definitions import root, BLOCKSIZE, BINARY
from src.b_tree.b_tree import BTree


def parse_ptr_string(ptr):
    ptr = ptr.split("-")
    return int(ptr[0]), int(ptr[1])


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
        print('b-tree loaded.')


        # get starting node
        start_node  = btree.get_start_node(btree.root, self.where_val[0])
        print(start_node.key)
        print(start_node.ptr)

        block_count = int(len(os.listdir(self.table_path)))
        result = self.index_scan(start_node)
        print("Result: ")
        try:
            print(result[self.rows])
        except Exception as e:
            print("No results found")

    def index_scan(self, start_node):
        node = start_node
        start_time = time.time()
        block_num = int(start_node.ptr.split("-")[0])
        ptrs_df = pd.DataFrame(columns=['block','idx'])
        while node.key == self.where_val[0]:
            b,i = parse_ptr_string(node.ptr)
            ptrs_df = ptrs_df.append({'idx':i,'block':b}, ignore_index=True)
            node = node.next
            if not node:
                break


        ptrs_df = ptrs_df.sort_values('block')
        ptrs_df = ptrs_df.reset_index()
        print(ptrs_df)
        final_df = pd.DataFrame()
        block_num = 0
        if BINARY:
            current_block = pd.read_pickle(self.table_path+'_bin'+ "/block" + str(block_num) + ".p").reset_index()
        else:
            current_block = pd.read_csv(self.table_path+'/block'+str(block_num)+'.csv')
        self.block_reads +=1
        for df_idx, row in ptrs_df.iterrows():
            block = int(row['block'])
            idx = int(row['idx'])
            if block > block_num + 1:
                self.seeks +=1
            if block_num != block:
                block_num = block
                if BINARY:
                    current_block = pd.read_pickle(self.table_path+'_bin'+ "/block" + str(block_num) + ".p").reset_index()
                else:
                    current_block = pd.read_csv(self.table_path+'/block'+str(block_num)+'.csv')
                self.block_reads += 1
            new_row = current_block.loc[idx]
            # new_row = pd.concat([new_row,df.loc[df_idx]]).to_dict()
            final_df = final_df.append(new_row,ignore_index=True)
        # final_df = final_df.drop(columns=['block', 'idx','index'])            # drop extra columns


        run_time = time.time() - start_time
        print("Runtime: "+"%.3f" % run_time+ " Seconds")
        print("Block Transfers: " +str(self.block_reads))
        print("Seeks:", self.seeks)

        if len(final_df.index):
            return final_df
