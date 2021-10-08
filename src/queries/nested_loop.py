""" plain nested loop """
import os.path
import time
import pandas as pd
from definitions import root


class NestedLoop:
    def __init__(self, data_version, tables, where, where_col, where_op, where_val):
        self.data_version = data_version
        self.query_tables = tables
        self.where = where
        self.where_col = where_col
        self.where_op = where_op
        self.where_val = where_val
        self.DB_PATH = os.path.join(root, 'data', self.data_version, 'disk')
        block_list = self.tables()
        result = self.nested(block_list)
        print("Result: ")
        print(result.to_string(index=False))

    def tables(self):
        block_list = []
        for table in self.query_tables:
            table_path = os.path.join(self.DB_PATH, table)
            block_count = len(os.listdir(table_path))
            block_list.append(int(block_count / 2))
        return block_list

    def nested(self, block_list):
        start_time = time.time()
        result_df = pd.DataFrame()
        block_reads, row = 0, 0
        for i in range(0, block_list[0]):
            outer_block = pd.read_csv(self.DB_PATH + "/" + self.query_tables[0] + "/block" + str(i) + ".csv")
            block_reads += 1
            for outer_index, outer_row in outer_block.iterrows():
                n = 0
                while n < block_list[1]:
                    inner_block = pd.read_csv(self.DB_PATH + "/" + self.query_tables[1] + "/block" + str(n) + ".csv")
                    block_reads += 1
                    for inner_index, inner_row in inner_block.iterrows():
                        if len(self.where_col) == 1:
                            if outer_row[self.where_col[0]] == inner_row[self.where_col[0]] \
                                    and inner_row[self.where_col[0]] == self.where_val[0] \
                                    and outer_row["Athlete ID"] == inner_row["Athlete ID"]:
                                merge = pd.concat([outer_row, inner_row]).drop_duplicates()
                                result_df = result_df.append(merge, ignore_index=True)
                        elif len(self.where_col) == 2:
                            if outer_row[self.where_col[0]] == inner_row[self.where_col[0]] \
                                    and outer_row[self.where_col[1]] == inner_row[self.where_col[1]] \
                                    and inner_row[self.where_col[0]] == self.where_val[0] \
                                    and inner_row[self.where_col[1]] == self.where_val[1] \
                                    and outer_row["Athlete ID"] == inner_row["Athlete ID"]:
                                merge = pd.concat([outer_row, inner_row]).drop_duplicates()
                                result_df = result_df.append(merge, ignore_index=True)
                    n += 1
                row += 1
        run_time = time.time() - start_time
        print("Runtime: " + "%.3f" % run_time + " Seconds")
        print("Block Transfers: " + str(block_reads))
        print("Seeks: " + str(row + block_list[0]))
        return result_df

