""" plain nested loop """
import os.path
import pandas as pd


class NestedLoop:
    def main(self):
        DB_PATH = '../../data/develop/disk/'
        table_dict = {}
        tables = os.listdir(DB_PATH)
        table_dict = self.tables(DB_PATH, table_dict, tables)
        self.nested(tables, table_dict)

    def tables(self, DB_PATH, table_dict, tables):
        for table in tables:
            table_path = os.path.join(DB_PATH, table)
            block_count = len(os.listdir(table_path))
            block = 0
            block_list = []
            for i in range(0, block_count):
                block_list.append(table_path+"/block"+str(block)+".csv")
                block += 1
            table_dict[table] = pd.concat(map(pd.read_csv, block_list), ignore_index=True)
        return table_dict

    def nested(self, tables, table_dict):
        result_df = pd.DataFrame()
        for outer_index, outer_row in table_dict[tables[0]].iterrows():
            for inner_index, inner_row in table_dict[tables[1]].iterrows():
                if outer_row["NOC"] == inner_row["NOC"] and (inner_row["NOC"] == "CHN" or inner_row["NOC"] == "JPN")\
                        and outer_row["Athlete ID"] == inner_row["Athlete ID"]:
                    merge = pd.concat([outer_row, inner_row]).drop_duplicates()
                    result_df = result_df.append(merge, ignore_index=True)

        print(result_df)


if __name__ == "__main__":
    nested_loop = NestedLoop()
    nested_loop.main()