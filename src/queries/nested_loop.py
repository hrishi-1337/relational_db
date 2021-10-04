""" plain nested loop """
import os.path
import pandas as pd


class NestedLoop:
    def main(self):
        DB_PATH = '../../data/develop/disk/'
        table_dict = {}
        NOC = "CHN"
        tables = os.listdir(DB_PATH)
        result = self.tables(DB_PATH, table_dict, tables, NOC)
        print(result)

    def tables(self, DB_PATH, table_dict, tables, NOC):
        for table in tables:
            table_path = os.path.join(DB_PATH, table)
            block_count = len(os.listdir(table_path))
            block = 0
            block_list = []
            for i in range(0, int(block_count/2)):
                block_list.append(table_path+"/block"+str(block)+".csv")
                block += 1
            table_dict[table] = pd.concat(map(pd.read_csv, block_list), ignore_index=True)
        result = self.nested(tables, table_dict, NOC)
        return result

    def nested(self, tables, table_dict, NOC):
        result_df = pd.DataFrame()
        for outer_index, outer_row in table_dict[tables[0]].iterrows():
            for inner_index, inner_row in table_dict[tables[1]].iterrows():
                if outer_row["NOC"] == inner_row["NOC"] and inner_row["NOC"] == NOC\
                        and outer_row["Athlete ID"] == inner_row["Athlete ID"]:
                    merge = pd.concat([outer_row, inner_row]).drop_duplicates()
                    result_df = result_df.append(merge, ignore_index=True)

        return result_df


if __name__ == "__main__":
    nested_loop = NestedLoop()
    nested_loop.main()