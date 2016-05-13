#!/home/hduser/anaconda/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
import re
from itertools import combinations 
from operator import itemgetter

class HW541(MRJob):

    TopFrequentUnigrams = []
    
    def steps(self):
        return [MRStep(mapper_init = self.mapper_init, 
                       mapper = self.mapper, combiner = self.combiner, 
                       reducer_init = self.reducer_init, 
                       reducer = self.reducer)]
    
    ## pull in the top occurring words dictionary here for the mapper
    def mapper_init(self):
        f = open("Top10kWords.txt","r")
        for unigram in f:
            unigram = unigram.strip()
            self.TopFrequentUnigrams.append(unigram)
        
    def mapper(self, _, line):
        cooccur = {}
        line.strip()
        [ngram,count,pages,books] = re.split("\t",line)
        # Output the count for each word in the 5-gram
        unigrams = ngram.split()
        # Get all of the 2-sets
        combs = list(combinations(unigrams,2))
        for combination in combs:
            unigram1,unigram2 = combination
            if unigram1 in self.TopFrequentUnigrams and \
                unigram2 in self.TopFrequentUnigrams:
                    cooccur.setdefault(unigram1,{})
                    cooccur[unigram1].setdefault(unigram2,0)
                    cooccur[unigram1][unigram2] += int(count)
                    cooccur.setdefault(unigram2,{})
                    cooccur[unigram2].setdefault(unigram1,0)
                    cooccur[unigram2][unigram1] += int(count)
        for unigram1 in cooccur.keys():
            yield unigram1, cooccur[unigram1]
        
    def combiner(self, unigram1, values):
        cooccur = {}
        for stripe in values:
            for unigram2 in stripe.keys():
                cooccur.setdefault(unigram2,0)
                cooccur[unigram2] += stripe[unigram2]
        yield unigram1, cooccur

    def reducer_init(self):
        f = open("Top10kWords.txt","r")
        for word in f:
            word = word.strip()
            self.TopFrequentUnigrams.append(word)
    
    def reducer(self, unigram1, values):
        cooccur = {}
        for stripe in values:
            for unigram2 in stripe.keys():
                cooccur.setdefault(unigram2,0)
                cooccur[unigram2] += stripe[unigram2]
        for unigram2 in self.TopFrequentUnigrams:
            cooccur.setdefault(unigram2,0)
        yield unigram1,','.join([str(cooccur[unigram2]) for unigram2 in \
                                  sorted(self.TopFrequentUnigrams)])

if __name__ == '__main__':
    HW541.run()