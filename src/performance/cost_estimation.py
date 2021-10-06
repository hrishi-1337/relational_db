import os.path
import pandas as pd
from definitions import root, BLOCKSIZE


class CostEstimation:

    def main(self):
        tables = ['compact_athletes', 'compact_athletic_events']
        self.nestedloop(tables)
        self.blocknestedloop(tables)

    def blocks(self, tables):
        DB_PATH = os.path.join(root, 'data', 'develop', 'disk')
        block_count = {}
        for table in tables:
            table_path = os.path.join(DB_PATH, table)
            blocks = len(os.listdir(table_path))
            block_count[table] = int(blocks / 2)
        return block_count, DB_PATH

    def nestedloop(self, tables):
        block_count, DB_PATH = self.blocks(tables)
        last_block = pd.read_csv(DB_PATH + "/" + tables[0] + "/block" + str(block_count[tables[0]]-1) + ".csv")
        outer_relations = (BLOCKSIZE * (block_count[tables[0]]-1)) + len(last_block.index)
        print("Nested loop estimated costs:")
        print("Block Transfers: " + str((outer_relations*block_count[tables[1]])+block_count[tables[0]]))
        print("Seeks: " + str(outer_relations+block_count[tables[0]]))


    def blocknestedloop(self, tables):
        block_count, DB_PATH = self.blocks(tables)
        print("Block nested loop estimated costs:")
        print("Block Transfers: " + str((block_count[tables[0]]*block_count[tables[1]])+block_count[tables[0]]))
        print("Seeks: " + str(2*block_count[tables[0]]))


if __name__ == "__main__":
    cost_estimation = CostEstimation()
    cost_estimation.main()
