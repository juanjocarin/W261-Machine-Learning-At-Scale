#!/usr/bin/python
import sys
import re

f = open('dictionary', 'r')
word_dict = []
for line in f:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split()
    for word in words:
        word_dict.append(word)

# input comes from STDIN (standard input)
for line in sys.stdin:
    word_count = 0 # count of word in the email
    ID = line.split("\t")[0]
    TRUTH = line.split("\t")[1]
    content = ' '.join(line.strip().split("\t")[2:])
        # We search the word in both the subject and the content
            # because one or the other may not exist, but the way the data are
            # stored we don't know which one may be missing
    content = re.sub('[^a-z]', ' ', content.lower())
    words = content.split() # extract words
    for word in set(word_dict):
        print word + '\t' + str(words.count(word)) + '\t' + ID + '\t' + TRUTH