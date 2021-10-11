""" pytests for btree functionality """

import os
from src.b_tree.b_tree import BTree

# initialize trees
dev_tree = BTree(index_file='data/develop/indexes/Team', col="Team", num_children=10)
dev_tree_bin = BTree(index_file='data/develop/indexes/Team', col="Team", num_children=10, binary=True)

def test_dev_depth():
    assert dev_tree.get_depth(dev_tree.root)==4

def test_dev_length():
    assert dev_tree.get_num_leaves()==189

def test_dev_bin_depth():
    assert dev_tree_bin.get_depth(dev_tree_bin.root)==4

def test_dev_bin_length():
    assert dev_tree_bin.get_num_leaves()==189
