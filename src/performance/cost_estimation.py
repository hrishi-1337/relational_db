import os.path
import pandas as pd
from definitions import root, BLOCKSIZE


class CostEstimation:
    def blocks(self, tables):
        DB_PATH = os.path.join(root, 'data', 'develop', 'disk')
        block_count = {}
        for table in tables:
            table_path = os.path.join(DB_PATH, table)
            blocks = len(os.listdir(table_path))
            block_count[table] = int(blocks / 2)
        return block_count, DB_PATH

    def cost(self, tables):
        block_count, DB_PATH = self.blocks(tables)
        df = pd.DataFrame(index=['Block Transfers:', 'Seeks:'], columns=['Nested Loop Join', 'Block Nested Loop Join'])
        last_block = pd.read_csv(DB_PATH + "/" + tables[0] + "/block" + str(block_count[tables[0]]-1) + ".csv")
        outer_relations = (BLOCKSIZE * (block_count[tables[0]]-1)) + len(last_block.index)
        df.at['Block Transfers:', 'Nested Loop Join'] = \
            str((outer_relations*block_count[tables[1]])+block_count[tables[0]])
        df.at['Seeks:', 'Nested Loop Join'] = str((outer_relations*block_count[tables[1]])+block_count[tables[0]])
        df.at['Block Transfers:', 'Block Nested Loop Join'] = \
            str((block_count[tables[0]]*block_count[tables[1]])+block_count[tables[0]])
        df.at['Seeks:', 'Block Nested Loop Join'] = str(2*block_count[tables[0]])
        print(df.to_string())
        return df
