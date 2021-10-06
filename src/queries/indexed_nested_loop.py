""" indexed nested loop """
import pandas as pd
import os
from src.b_tree.b_tree import BTree
from termcolor import colored
from sys import exit
from pprint import pprint
import time

def get_cols(table):
    cols = []
    if table in os.listdir('data/develop/disk/'):
        with open('data/develop/disk/'+table+'/block0.csv', 'r') as f:
            for line in f:
                cols += line[:-1].split(",")
                break
    elif table in os.listdir('data/full/disk/'):
        with open('data/full/disk/'+table+'/block0.csv', 'r') as f:
            for line in f:
                cols += line[:-1].split(",")
                break
    if not cols:
        print(colored('error:', 'red'),end=" ")
        print(f"can't retrieve columns from {table}")
    cols = [x.lower() for x in cols]
    return cols


def parse_ptr_string(ptr):
    ptr = ptr.split("-")
    return int(ptr[0]), int(ptr[1])


class IndexedJoin:
    def __init__(self, data_version, table1, table2, join_col, where=False, where_col=None, where_op=None, where_val = None):
        # data_version: full or develop
        # table 1:      first table
        # table 2:      second table to join on
        # join_col:     column to join on (joins for equal column attributes)
        # where:        column for where condition, False if no where condition
        # where_op:     operator for where condtion(<,>,=), None if no where condition
        # where_val:    value for where condition, None if no where condition

        self.data_version = data_version
        self.table1 = table1
        self.table2 = table2
        self.join_col_orignal = join_col
        self.join_col = join_col.lower()
        self.where = where
        self.where_op = where_op
        self.where_val = where_val

        table1_path = os.path.join('data', data_version, 'disk', table1)
        table2_path = os.path.join('data', data_version, 'disk', table2)

        # metrics
        self.block_reads = 0

        # concat paths to index
        index1_path = os.path.join('data',data_version,'indexes',table1+'_'+join_col)
        index2_path = os.path.join('data',data_version,'indexes',table2+'_'+join_col)

        # if index join was called, the index must exist for both tables
        assert os.path.exists(index1_path+'.csv'), f"no index for {table2}"
        assert os.path.exists(index2_path+'.csv'), f"no index for {table1}"

        # join col needs to be unqiue in one of the columns
        assert self.join_col.lower() in [get_cols(table1)[0].replace(" ", "_"), get_cols(table2)[0].replace(" ", "_")], "one of the indexes must be unique"

        # load indexes into b-trees for both tables
        btree1 = BTree(index1_path, join_col)
        btree2 = BTree(index2_path, join_col)


        # set inner table to table where join_col is unique
        if self.join_col == get_cols(table1)[0].replace(" ", "_"):        # table 1 index is unique
            self.outer = btree2
            self.inner = btree1
            self.outer_path = table2_path
            self.inner_path = table1_path
        else:                                           # table 2 index is unique
            self.outer = btree1
            self.inner = btree2
            self.outer_path = table1_path
            self.inner_path = table2_path



    def join(self):
        inner = self.inner
        outer = self.outer

        # inorder to process the where clause simultanously as the join,
        # the join column and the where column must be the same, because
        # the index only has the join column
        if self.where and self.where == self.join_col:
            return self.join_with_where(inner, outer)

        # in this function:
        #   1. creates dataframe that holds (ptr-> row in table1, ptr-> row in table 2) for each row
        #   2. uses pointer to load all needed blocks from inner table
        #   3. re-sorts dataframe by block for outer table
        #   4. uses pointer to load all needed blocks from outer table
        #   5. returns joined data


        # initialize dataframe for pointers
        ptrs_df = pd.DataFrame(columns=['inner','inner-idx','outer','outer-idx'])
        # because we are joining entire index, we just go to first node and iterate
        outer_node = outer.first()
        inner_node = inner.first()

        # adds rows to data frame that 'joins' the indexes of each table
        while outer_node.next != None:
            if inner_node.key == outer_node.key:
                inn,inn_idx = parse_ptr_string(inner_node.ptr)
                out, out_idx = parse_ptr_string(outer_node.ptr)
                ptrs_df = ptrs_df.append({'inner':inn, 'inner-idx':inn_idx, 'outer':out, 'outer-idx':out_idx}, ignore_index=True)
                outer_node = outer_node.next
            else:
                inner_node = inner_node.next

        # load blocks from inner table to dataframe
        #   - saves outer table pointer in each row
        df = pd.DataFrame()
        block_num = 0
        current_block = pd.read_csv(self.inner_path+'/block'+str(block_num)+'.csv')
        self.block_reads +=1
        for _, row in ptrs_df.iterrows():
            block = row['inner']
            idx = row['inner-idx']
            if block_num != block:
                block_num = block
                current_block = pd.read_csv(self.inner_path+'/block'+str(block_num)+'.csv')
                self.block_reads += 1
            new_row = current_block.loc[idx]
            new_row['ptr_block'] = row['outer']
            new_row['ptr_index'] = row['outer-idx']
            df = df.append(new_row)

        # sort values according to outside table pointer
        df = df.sort_values('ptr_block')
        df = df.reset_index()

        # new dataframe to load fully joined data
        final_df = pd.DataFrame()
        block_num = 0
        current_block = pd.read_csv(self.outer_path+'/block'+str(block_num)+'.csv')
        self.block_reads +=1
        for df_idx, row in df.iterrows():
            block = int(row['ptr_block'])
            idx = int(row['ptr_index'])
            if block_num != block:
                block_num = block
                current_block = pd.read_csv(self.outer_path+'/block'+str(block_num)+'.csv')
                self.block_reads += 1
            new_row = current_block.loc[idx]
            new_row = pd.concat([new_row,df.loc[df_idx]]).to_dict()
            final_df = final_df.append(new_row,ignore_index=True)
        final_df = final_df.drop(columns=['ptr_block', 'ptr_index','index'])            # drop extra columns

        return final_df, self.block_reads
