""" index data structure implementation """
import pandas as pd
from sys import exit
from math import ceil

class Node:
    def __init__(self, key,ptr):
        self.key = key
        self.ptr = ptr

class BTree:
    leaves = []
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

    def recursive_fill(self, node, i, j):
        # base case
        if j-i<=self.num_children:
            iter = i
            while iter < j:
                data = self.df.loc[iter]
                new_node = Node(data['Team'], data['ptr'])
                node.ptr.append(new_node)
                iter+=1

        else:
            inc = max(ceil((j-i)/self.num_children),self.num_children)
            for idx in range(i,j,inc):
                new_node = Node(self.df.loc[idx]['Team'],[])
                self.recursive_fill(new_node,idx, min(idx+inc,j))
                node.ptr.append(new_node)

    def add_sideways_ptrs(self, node):
        if isinstance(node.ptr,str):
            self.leaves.append(node)
        else:
            for n in node.ptr:
                self.add_sideways_ptrs(n)

    def update_ptrs(self):
        leaves = self.leaves
        for i in range(len(leaves)):
            if i == len(leaves)-1:
                leaves[i].next = None
            else:
                leaves[i].next = leaves[i+1]

    def print_tree(self, node, depth):
        if type(node.ptr) == type(''):
            print(depth, node.key, node.ptr)
        else:
            # print(depth,"INNER:",node.key)
            for child in node.ptr:
                self.print_tree(child,depth+1)

    def horizontal_print(self):
        temp = self.root
        while not isinstance(temp.ptr, str):
            temp = temp.ptr[0]
        while temp.next:
            print(temp.key,temp.ptr)
            temp = temp.next

    def get_depth(self, node):
        if type(node.ptr) == type(''):
            return 0
        else:
            return self.get_depth(node.ptr[0])+1


index_file = '../../data/develop/indexes/Team.csv'
x = BTree(index_file, num_children=10)
# x.print_tree(x.root,0)
print(x.get_depth(x.root))
x.horizontal_print()
