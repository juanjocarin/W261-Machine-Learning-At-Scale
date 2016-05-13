#!/usr/bin/python
import sys
import re
import os

# Word to use find/use
env_vars = os.environ
findword = env_vars['findword']

WORD_RE = re.compile(r"[\w']+")
word_count = 0

# input comes from STDIN (standard input)
for line in sys.stdin:
    for w in WORD_RE.findall(line):
        if findword.lower() == w.lower():
            word_count += 1
print findword + '\t' + str(word_count)