import os.path
import time
import pandas as pd
from definitions import root, BLOCKSIZE, BINARY


class LinearScan:
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

        self.seeks = 1

        block_count = int(len(os.listdir(self.table_path)))
        result = self.linear_scan(block_count)
        print("Result: ")
        try:
            print(result[self.rows])#.to_string(index=False))
        except Exception as e:
            print("No results found")

    def linear_scan(self, block_count):
        start_time = time.time()
        result = pd.DataFrame()
        block_reads = 0
        for i in range(block_count-1):
            if BINARY:
                block = pd.read_pickle(self.table_path+'_bin'+ "/block" + str(i) + ".p")
            else:
                block = pd.read_csv(self.table_path + "/block" + str(i) + ".csv")
            block_reads += 1
            for j in range(BLOCKSIZE):
                    if block.iloc[j][self.where_col[0]] == self.where_val[0]:
                        result = result.append(block.iloc[j], ignore_index=True)
        run_time = time.time() - start_time
        print("Runtime: "+"%.3f" % run_time+ " Seconds")
        print("Block Transfers: " +str(block_reads))
        print("Seeks: ", self.seeks)
        return result
