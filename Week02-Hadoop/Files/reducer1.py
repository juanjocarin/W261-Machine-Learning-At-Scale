#!/usr/bin/python
import sys

vocabulary = []
for line in sys.stdin:
    # Take key only (the word) and add to vocabulary if not present
    word = line.split("\t")[0]
    #if word not in vocabulary:
    #    print word
    # If we use the 2 lines above instead of the 3 lines below
        # each word in the vocabulary goes in a new line
        # (and there's no need to sort)
    vocabulary.append(word)
vocabulary = sorted(set(vocabulary)) # Get unique words
print ' '.join(vocabulary) # Print words separated by space