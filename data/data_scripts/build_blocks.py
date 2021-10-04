""" script to create block files from original """
import os
import shutil
import pandas as pd
import pickle

""" reads in csv files in 'no_block_data' and creates a new directory for each file
	then writes file to file"""
def make_blocks(dir,BLOCK_SIZE,csv=True,binary=True):
	remake=True

	if not os.path.exists(dir+'disk'):
		os.mkdir(dir+'disk')

	# delete and remake directory
	if remake:
		shutil.rmtree(dir+'disk')
		os.mkdir(dir+'disk')

	# loop through csv files for each new table
	for csv in os.listdir(dir+'no_block_data'):
		table_name = csv[:-4]
		print('making block files for table '+table_name+'...')
		if not os.path.isdir(dir+'disk/'+table_name):
			os.mkdir(dir+'disk/' + table_name)
			os.mkdir(dir+'disk/' + table_name+'_bin')
		df = pd.read_csv(dir+'no_block_data/'+csv)
		data_len = len(df.index)
		print("     table len",data_len,"block size", BLOCK_SIZE)
		block_num = 0

		# write each block to new file
		for idx in range(0,data_len,BLOCK_SIZE):
			filename = '/block'+str(block_num)
			newdf = df[idx:idx+BLOCK_SIZE]
			if csv:
				newdf.to_csv(dir+'disk/'+table_name+filename+'.csv', index=False)
			if binary:
				newdf.to_pickle(dir+'disk/'+table_name+'_bin'+filename+'.p')
			block_num+=1
