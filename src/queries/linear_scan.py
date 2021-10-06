import os.path
import time
import pandas as pd
from definitions import root, BLOCKSIZE


class LinearScan:
    def main(self):
        NOC = "CHN"
        table = "compact_athletes"
        table_path = os.path.join(root, 'data', 'develop', 'disk', table)
        block_count = int(len(os.listdir(table_path))/2)
        result = self.linear_scan(table_path, block_count, NOC)
        print(result)

    def linear_scan(self, table_path, block_count, NOC):
        start_time = time.time()
        result = pd.DataFrame()
        block_reads = 0
        for i in range(0, block_count-1):
            block = pd.read_csv(table_path + "/block" + str(i) + ".csv")
            block_reads += 1
            for j in range(0, BLOCKSIZE-1):
                    if block.iloc[j]["NOC"] == NOC:
                        result = result.append(block.iloc[j], ignore_index=True)
        run_time = time.time() - start_time
        print("Runtime: "+"%.3f" % run_time+ " Seconds")
        print("Block Transfers: " +str(block_reads))
        print("Seeks: "+str(len(result.index)))
        return result


if __name__ == "__main__":
    linear_scan = LinearScan()
    linear_scan.main()
