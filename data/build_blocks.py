""" script to create block files from original """

import os
import shutil
import pandas as pd
from sys import exit

BLOCK_SIZE = 10
build = ['develop'] # either 'full', 'develop', or both
remake = True		# to remake all tables
REPO_PATH = '/Users/wesrobbins/Desktop/fall2021/540/hw1-repo/' # path to repo on coputer

# cd into repo
os.chdir(REPO_PATH+'data')



""" reads in csv files in 'no_block_data' and creates a new directory for each file
	then writes file to file"""
def make_blocks(dir):
	os.chdir(dir)

	if not os.path.exists('./disk'):
		os.mkdir('./disk')

	# delete and remake directory
	if remake:
		shutil.rmtree('./disk')
		os.mkdir('./disk')


	for csv in os.listdir('./no_block_data'):
		table_name = csv[:-4]
		print(table_name)
		if not os.path.isdir('./disk/'+table_name):
			print("here")
			os.mkdir('./disk/' + table_name)
		df = pd.read_csv('./no_block_data/'+csv)
		# print(df)
		data_len = len(df.index)
		block_num = 0
		for idx in range(0,data_len,BLOCK_SIZE):
			filename = '/block'+str(block_num)+'.csv'
			df[idx:idx+BLOCK_SIZE].to_csv('./disk/'+table_name+filename)
			block_num+=1




for db in build:
	make_blocks(db)
