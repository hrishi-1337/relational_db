""" index data structure implementation """
# Wes Robbins Oct. 2


import pandas as pd
from sys import exit
from math import ceil

# node of btree
class Node:
    def __init__(self, key,ptr):
        self.key = key              # key
        self.ptr = ptr              # ptr is a list of child nodes, unless it is leaf, then ptr is a str with block #

class BTree:
    leaves = []                                             # stores leaves for adding horizontal pointers
    def __init__(self, index_file, num_children=10):
        # read in index from file
        self.df = pd.read_csv(index_file,index_col=False)

        # length and midpoint
        index_len = len(self.df.index)
        mp = int(index_len/num_children)*int(num_children/2)
        self.num_children = num_children

        # initialize root of tree
        self.root = Node(self.df.loc[mp]['Team'],[])

        # call recursive function to fill tree
        self.recursive_fill(self.root, 0, index_len)

        # add horizontal pointers to leaf nodes
        self.add_sideways_ptrs(self.root)
        self.update_ptrs()

    # recursively fill tree from pandas dataframe
    def recursive_fill(self, node, i, j):
        # i: beginning index
        # j: end index
        # node: current node

        # base case
        if j-i<=self.num_children:
            iter = i
            while iter < j:
                data = self.df.loc[iter]
                node.ptr.append(Node(data['Team'], data['ptr']))    # create leaf nodes
                iter+=1

        # if not leaf node
        else:
            inc = max(ceil((j-i)/self.num_children),self.num_children)
            for idx in range(i,j,inc):
                self.recursive_fill(Node(self.df.loc[idx]['Team'],[]),idx, min(idx+inc,j))
                node.ptr.append(new_node)

    # recursively add leaf nodes to self.leaves
    def add_sideways_ptrs(self, node):
        if isinstance(node.ptr,str):
            self.leaves.append(node)
        else:
            for n in node.ptr:
                self.add_sideways_ptrs(n)

    # go through self.leaves and add pointers to next leaf
    def update_ptrs(self):
        leaves = self.leaves
        for i in range(len(leaves)):
            if i == len(leaves)-1:
                leaves[i].next = None
            else:
                leaves[i].next = leaves[i+1]

    # print ordered data standard way
    def print_tree(self, node, depth):
        if isinstance(node.ptr,str):
            print(depth, node.key, node.ptr)
        else:
            for child in node.ptr:
                self.print_tree(child,depth+1)

    # print ordered data using leaf pointers
    def horizontal_print(self):
        temp = self.root
        while not isinstance(temp.ptr, str):
            temp = temp.ptr[0]
        while temp.next:
            print(temp.key,temp.ptr)
            temp = temp.next

    # return depth of tree
    def get_depth(self, node):
        if type(node.ptr) == type(''):
            return 0
        else:
            return self.get_depth(node.ptr[0])+1


index_file = '../../data/develop/indexes/Team.csv'
x = BTree(index_file, num_children=10)
print(x.get_depth(x.root))
x.horizontal_print()
