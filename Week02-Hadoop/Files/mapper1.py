#!/usr/bin/python
import sys
import re
import os

vocabulary = []

# input comes from STDIN (standard input)
for line in sys.stdin:
    content = ' '.join(line.strip().split("\t")[2:])
    # We search the word in both the subject and the content
        # because one or the other may not exist, but the way the data are
        # stored we don't know which one may be missing
    content = re.sub('[^a-z]', ' ', content.lower())
    # Discard non-alphanumeric characters and also numbers
    words = content.split() # extract words
    words = set(words) # extract unique words
    vocabulary[1:1] = words # append to vocabulary
for word in set(vocabulary):
    print '%s\t%s' % (word, 1) # value here is not important