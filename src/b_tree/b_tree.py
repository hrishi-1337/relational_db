""" index data structure implementation """
# Wes Robbins Oct. 2


import pandas as pd
from sys import exit
from math import ceil
import yaml

# node of b-tree
class Node:
    def __init__(self, key,ptr):
        self.key = key              # key
        self.ptr = ptr              # ptr is a list of child nodes, unless it is leaf, then ptr is a str with block #

class BTree:                                            # stores leaves for adding horizontal pointers
    def __init__(self, index_file, col,num_children=10, binary=False):
        # read in index from file
        if binary:
            self.df = pd.read_pickle(index_file+'.p').reset_index()
        else:
            self.df = pd.read_csv(index_file+'.csv',index_col=False)

        # length and midpoint
        index_len = len(self.df.index)
        mp = int(index_len/num_children)*int(num_children/2)
        self.num_children = num_children

        # initialize root of tree
        self.root = Node(self.df.loc[mp][col],[])
        self.leaves = []
        self.count = 0
        self.col = col

        # call recursive function to fill tree
        self.recursive_fill(self.root, 0, index_len)

        # add horizontal pointers to leaf nodes
        self.add_sideways_ptrs(self.root)
        self.update_ptrs()

    def get_start_node(self, node, val):
        if isinstance(node.ptr,str):
            return node
        elif isinstance(node.ptr[0].ptr,str):
            for i in range(len(node.ptr)):
                if node.ptr[i].key == val:
                    return self.get_start_node(node.ptr[i], val)
            return self.get_start_node(node.ptr[len(node.ptr)-1], val)

        else:
            for i in range(len(node.ptr)-1):
                if node.ptr[i+1].key >= val:
                    return self.get_start_node(node.ptr[i], val)
            return self.get_start_node(node.ptr[len(node.ptr)-1], val)


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
                node.ptr.append(Node(data[self.col], data['ptr']))    # create leaf nodes
                iter+=1
                self.count+=1

        # if not leaf node
        else:
            inc = max(ceil((j-i)/self.num_children),self.num_children)
            for idx in range(i,j,inc):
                node.ptr.append(Node(self.df.loc[idx][self.col],[]))
                self.recursive_fill(node.ptr[-1],idx, min(idx+inc,j))


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
            return 1
        else:
            return self.get_depth(node.ptr[0])+1

    # count leaves in tree
    def get_num_leaves(self):
        temp = self.root
        while not isinstance(temp.ptr, str):
            temp = temp.ptr[0]
        count = 0
        while temp.next:
            count += 1
            temp = temp.next
        return count + 1

    def first(self):
        f = self.root
        while not isinstance(f.ptr, str):
            f = f.ptr[0]
        return f


# index_file = '../../data/develop/indexes/Team'
# x = BTree(index_file, num_children=10, binary=True)
# print(x.get_depth(x.root))
