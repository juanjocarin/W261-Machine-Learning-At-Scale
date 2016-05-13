#!/home/hduser/anaconda/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class FreqUnigrams(MRJob):
     
    def steps(self):
        return [MRStep(mapper = self.mapper, combiner = self.combiner, 
                       reducer_init = self.reducer_init, 
                       reducer = self.reducer, 
                       reducer_final = self.reducer_final)]
    
    def mapper(self, _, line):
        line.strip()
        [ngram,count,pages,books] = re.split("\t",line)
        # Output the count for each word in the 5-gram
        for unigram in ngram.split():
            yield unigram,int(count)

    # Aggregate partial results before passing to reducer
    def combiner(self, unigram, count):
        partial = sum(c for c in count)
        yield unigram,int(partial)
            
    # Initialize a dictionary with top10 (initially 10 arbitrary keys 
        # whose value is 0)
    def reducer_init(self):
        self.top = {}
        import string
        for i in string.lowercase[:10]:
            self.top[i]=0

    def reducer(self,unigram,partial):
        # Aggregate counts
        total = sum(p for p in partial)
        # If higher than what's already in the Top10...
        if total > min(self.top.values()):
            # remove minimum value
            self.top.pop(min(self.top, key=self.top.get))
            # Substitute with new one
            self.top[unigram] = total

    # Output only Top10
    def reducer_final(self):
        for k,v in self.top.iteritems():
            yield v,k
    
if __name__ == '__main__':
    FreqUnigrams.run()