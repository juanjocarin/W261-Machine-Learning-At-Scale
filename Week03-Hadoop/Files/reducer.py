#!/usr/bin/python
import sys
import collections
import ast
from operator import itemgetter

# Create a dictionary with item1 as keys
primary_keys={}

# input comes from STDIN (standard input)
for line in sys.stdin:
    itemset = line.split('\t') # each line corresponds to an itemset
    item1 = itemset[0] # first part is the item1 (primary key)
    # Create the key if found new product
    if item1 not in primary_keys.keys():
        primary_keys[item1] = collections.Counter()
    # Add/Update the dictionary corresponding to that key
    primary_keys[item1].update(ast.literal_eval(itemset[1]))
    # Since we've used the STRIPES approach we have all the data we need

frequent_itemsets = []
# For each key (one line per item1 => STRIPES)
for item1 in primary_keys.keys():
    # For each item2
    for item2 in primary_keys[item1].keys():
        if item2 != '*':
            # For itemsets with Support > 100
            if float(primary_keys[item1][item2]) > 100:
                # Estimate the Confidence
                frequent_itemsets.append((item1,item2, 
                                          float(primary_keys[item1][item2]) / \
                                          primary_keys[item1]['*']))

# Sort in ascending order
frequent_itemsets.sort(key=itemgetter(2), reverse=True)
# And report top 5 association rules (item1 => item2: confidence)
top = frequent_itemsets[:5]
for itemset in top:
    print '({}) => {}: {:.4f}'.format(itemset[0], itemset[1], itemset[2])