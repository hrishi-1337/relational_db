import pandas as pd
import os
import shutil


class relational_db:
	def main(self):
		sources = ["test1", "test2"]
		blocksize = 2
		disk_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "disk")
		table_list = self.read_csv(sources)
		self.write_blocks(blocksize, table_list, sources, disk_path)
		self.read_blocks()

	def read_csv(self, sources):
		table_list = []
		for source in sources:
			table = pd.read_csv("datasets/"+source+".csv")
			table_list.append(table)
		return table_list

	def write_blocks(self, blocksize, table_list, sources, disk_path):
		for table, source in zip(table_list, sources):
			block = 1
			table_dir = os.path.join(disk_path, source)
			if os.path.exists(table_dir):
				shutil.rmtree(table_dir)
			os.mkdir(table_dir)
			df_list = [table.loc[i:i+blocksize-1,:] for i in range(0, len(table),blocksize)]
			for df in df_list:
				df.to_csv("disk/"+source+"/Block"+str(block))
				block += 1

	def read_blocks(self):
		table = pd.read_csv("disk/test1/Block1")
		print(table)
		
	def create_index(self):
		pass

	def nestedloop_join(self):
		pass

	def blocknestedloop_join(self):
		pass

	def indexnestedloop_join(self):
		pass

	def linear_scan(self):
		pass

	def blocktransfer_cost(self):
		pass

	def seeks_cost(self):
		pass


if __name__ == "__main__":
    relational_db = relational_db()
    relational_db.main()
