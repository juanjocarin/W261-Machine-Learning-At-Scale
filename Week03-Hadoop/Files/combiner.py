#!/usr/bin/python
import sys
import collections
import ast

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

# For each key (one line per item1 => STRIPES)
    # Same output as the mappers (but aggregated :D)
for k, v in primary_keys.items():
    print '%s\t%s' % (k,dict(collections.OrderedDict(sorted(v.items()))))