""" block nested loop """
import os.path
import time
import pandas as pd
from definitions import root,KEY


class BlockNestedLoop:
    def __init__(self, data_version, tables, rows, where, where_col, where_op, where_val):
        self.data_version = data_version
        self.rows = rows
        self.query_tables = tables
        self.where = where
        self.where_col = where_col
        self.where_op = where_op
        self.where_val = [i.replace("_", " ") for i in where_val]
        self.DB_PATH = os.path.join(root, 'data', self.data_version, 'disk')
        block_list, columns = self.tables()
        result = self.block_nested(block_list, columns)
        print("Result: " )
        try:
            print(result[self.rows])#.to_string(index=False))
        except Exception as e:
            print("No results found")

    def tables(self):
        block_list = []
        columns = {}
        for table in self.query_tables:
            table_path = os.path.join(self.DB_PATH, table)
            block_count = len(os.listdir(table_path))
            block_list.append(int(block_count / 2))
            with open(os.path.join(self.DB_PATH, table, 'block0.csv'), 'r') as f:
                for line in f:
                    columns[table] = line[:-1].split(",")
                    break
        return block_list, columns

    def block_nested(self, block_list, columns):
        start_time = time.time()
        result_df = pd.DataFrame()
        block_reads, row = 0, 0
        for i in range(0, block_list[0]):
            outer_block = pd.read_csv(self.DB_PATH+"/"+self.query_tables[0]+"/block"+str(i)+".csv")
            block_reads += 1
            for j in range(0, block_list[1]):
                inner_block = pd.read_csv(self.DB_PATH + "/" + self.query_tables[1] + "/block" + str(j) + ".csv")
                block_reads += 1
                for outer_index, outer_row in outer_block.iterrows():
                    for inner_index, inner_row in inner_block.iterrows():
                        if self.where:
                            if self.where_col[0] in columns[self.query_tables[0]] and self.where_col[0] in columns[
                                self.query_tables[1]]:
                                if outer_row[self.where_col[0]] == inner_row[self.where_col[0]] \
                                        and inner_row[self.where_col[0]] == self.where_val[0] \
                                        and outer_row[KEY] == inner_row[KEY]:
                                    merge = pd.concat([outer_row, inner_row]).drop_duplicates()
                                    result_df = result_df.append(merge.to_dict(), ignore_index=True)
                            elif self.where_col[0] in columns[self.query_tables[0]]:
                                if outer_row[self.where_col[0]] == self.where_val[0] \
                                        and outer_row[KEY] == inner_row[KEY]:
                                    merge = pd.concat([outer_row, inner_row]).drop_duplicates()
                                    result_df = result_df.append(merge.to_dict(), ignore_index=True)
                            elif self.where_col[0] in columns[self.query_tables[1]]:
                                if inner_row[self.where_col[0]] == self.where_val[0] \
                                        and inner_row[KEY] == outer_row[KEY]:
                                    merge = pd.concat([outer_row, inner_row]).drop_duplicates()
                                    result_df = result_df.append(merge.to_dict(), ignore_index=True)
                        else:
                            if outer_row[KEY] == inner_row[KEY]:
                                merge = pd.concat([outer_row, inner_row]).drop_duplicates()
                                result_df = result_df.append(merge.to_dict(), ignore_index=True)
                    row += 1
        run_time = time.time() - start_time
        print("Runtime: %.3f Seconds " % run_time)
        print("Block Transfers: " +str(block_reads))
        print("Seeks: " + str(2*block_list[0]))
        return result_df
