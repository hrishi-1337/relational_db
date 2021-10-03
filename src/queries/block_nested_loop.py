""" block nested loop """
import os.path
import pandas as pd


class BlockNestedLoop:
    def main(self):
        DB_PATH = '../../data/develop/disk/'
        block_list = []
        tables = os.listdir(DB_PATH)
        block_list = self.tables(DB_PATH, block_list, tables)
        self.block_nested(DB_PATH, tables, block_list)

    def tables(self, DB_PATH, block_list, tables):
        for table in tables:
            table_path = os.path.join(DB_PATH, table)
            block_count = len(os.listdir(table_path))
            block_list.append(block_count)
        return block_list

    def block_nested(self, DB_PATH, tables, block_list):
        result_df = pd.DataFrame()
        for i in range(0, block_list[0]):
            outer_block = pd.read_csv(DB_PATH+"/"+tables[0]+"/block"+str(i)+".csv")
            for j in range(0, block_list[1]):
                inner_block = pd.read_csv(DB_PATH + "/" + tables[1] + "/block" + str(j) + ".csv")
                for outer_index, outer_row in outer_block.iterrows():
                    for inner_index, inner_row in inner_block.iterrows():
                        if outer_row["NOC"] == inner_row["NOC"] and (inner_row["NOC"] == "CHN" or inner_row["NOC"] == "JPN")\
                                and outer_row["Athlete ID"] == inner_row["Athlete ID"]:
                            merge = pd.concat([outer_row, inner_row]).drop_duplicates()
                            result_df = result_df.append(merge, ignore_index=True)

        print(result_df)

if __name__ == "__main__":
    block_nested_loop = BlockNestedLoop()
    block_nested_loop.main()