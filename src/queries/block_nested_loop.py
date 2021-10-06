""" block nested loop """
import os.path
import time
import pandas as pd
from definitions import root

class BlockNestedLoop:
    def main(self):
        DB_PATH = os.path.join(root, 'data', 'develop', 'disk')
        NOC = "CHN"
        tables = os.listdir(DB_PATH)
        tables[:] = [x for x in tables if "bin" not in x]
        block_list = self.tables(DB_PATH, tables)
        result = self.block_nested(DB_PATH, tables, block_list, NOC)
        print(result)

    def tables(self, DB_PATH, tables):
        block_list = []
        for table in tables:
            table_path = os.path.join(DB_PATH, table)
            block_count = len(os.listdir(table_path))
            block_list.append(int(block_count/2))
        return block_list

    def block_nested(self, DB_PATH, tables, block_list, NOC):
        start_time = time.time()
        result_df = pd.DataFrame()
        block_reads, row = 0, 0
        for i in range(0, block_list[0]):
            outer_block = pd.read_csv(DB_PATH+"/"+tables[0]+"/block"+str(i)+".csv")
            block_reads += 1
            for j in range(0, block_list[1]):
                inner_block = pd.read_csv(DB_PATH + "/" + tables[1] + "/block" + str(j) + ".csv")
                block_reads += 1
                for outer_index, outer_row in outer_block.iterrows():
                    for inner_index, inner_row in inner_block.iterrows():
                        if outer_row["NOC"] == inner_row["NOC"] and inner_row["NOC"] == NOC\
                                and outer_row["Athlete ID"] == inner_row["Athlete ID"]:
                            merge = pd.concat([outer_row, inner_row]).drop_duplicates()
                            result_df = result_df.append(merge, ignore_index=True)
                    row += 1
        run_time = time.time() - start_time
        print("Runtime: "+"%.3f" % run_time+ " Seconds")
        print("Block Transfers: " +str(block_reads))
        print("Seeks: " + str(2*block_list[0]))
        return result_df


if __name__ == "__main__":
    block_nested_loop = BlockNestedLoop()
    block_nested_loop.main()