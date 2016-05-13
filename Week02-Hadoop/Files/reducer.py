#!/usr/bin/python
import sys

sum_words = 0
for line in sys.stdin:
    key_value = line.split('\t')
    # The key is the single word we're counting
    key = key_value[0]
    # And each value, its count from a mapper
    value = key_value[1]
    sum_words += int(value)
print key + '\t' + str(sum_words)