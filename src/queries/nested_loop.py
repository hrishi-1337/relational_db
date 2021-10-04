""" plain nested loop """
import os.path
import pandas as pd
from definitions import root

class NestedLoop:
    def main(self):
        DB_PATH = os.path.join(root, 'data', 'develop', 'disk')
        table_dict = {}
        NOC = "CHN"
        tables = os.listdir(DB_PATH)
        tables[:] = [x for x in tables if "bin" not in x]
        result = self.tables(DB_PATH, table_dict, tables, NOC)
        print(result)

    def tables(self, DB_PATH, table_dict, tables, NOC):
        block_list = []
        for table in tables:
            table_path = os.path.join(DB_PATH, table)
            block_count = len(os.listdir(table_path))
            block_list.append(int(block_count / 2))
        result = self.nested(DB_PATH, tables, block_list, NOC)
        return result

    def nested(self, DB_PATH, tables, block_list, NOC):
        result_df = pd.DataFrame()
        for i in range(0, block_list[0]):
            outer_block = pd.read_csv(DB_PATH + "/" + tables[0] + "/block" + str(i) + ".csv")
            n = 0
            for outer_index, outer_row in outer_block.iterrows():
                inner_block = pd.read_csv(DB_PATH + "/" + tables[1] + "/block" + str(n) + ".csv")
                for inner_index, inner_row in inner_block.iterrows():
                    if outer_row["NOC"] == inner_row["NOC"] and inner_row["NOC"] == NOC \
                            and outer_row["Athlete ID"] == inner_row["Athlete ID"]:
                        merge = pd.concat([outer_row, inner_row]).drop_duplicates()
                        result_df = result_df.append(merge, ignore_index=True)
            n += 1
        return result_df


if __name__ == "__main__":
    nested_loop = NestedLoop()
    nested_loop.main()