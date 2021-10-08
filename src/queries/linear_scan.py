import os.path
import time
import pandas as pd
from definitions import root, BLOCKSIZE


class LinearScan:
    def __init__(self, data_version, table, where, where_col, where_op, where_val):
        self.data_version = data_version
        self.where = where
        self.where_col = str(where_col)
        self.where_op = str(where_op)
        self.where_val = str(where_val)
        self.table_path = os.path.join(root, 'data', self.data_version, 'disk', table[0])
        block_count = int(len(os.listdir(self.table_path))/2)
        result = self.linear_scan(block_count)
        print("Result: ")
        print(result.to_string(index=False))

    def linear_scan(self, block_count):
        start_time = time.time()
        result = pd.DataFrame()
        block_reads = 0
        for i in range(0, block_count-1):
            block = pd.read_csv(self.table_path + "/block" + str(i) + ".csv")
            block_reads += 1
            for j in range(0, BLOCKSIZE-1):
                    if block.iloc[j][self.where_col] == self.where_val:
                        result = result.append(block.iloc[j], ignore_index=True)
        run_time = time.time() - start_time
        print("Runtime: "+"%.3f" % run_time+ " Seconds")
        print("Block Transfers: " +str(block_reads))
        print("Seeks: "+str(len(result.index)))
        return result