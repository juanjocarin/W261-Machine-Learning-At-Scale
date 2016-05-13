#!/home/hduser/anaconda/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class Density(MRJob):
     
    def steps(self):
        return [MRStep(mapper = self.mapper, combiner = self.combiner, 
                       reducer = self.reducer)]
    
    def mapper(self, _, line):
        line.strip()
        [ngram,count,pages,books] = re.split("\t",line)
        # Output the count for each word in the 5-gram
        for unigram in ngram.split():
            # Value: count & pages
            yield unigram,[int(count),int(pages)]

    # Aggregate partial results before passing to reducer
    def combiner(self, unigram, duple):
        partial_count = 0
        partial_pages = 0
        for count,pages in duple:
            partial_count += count
            partial_pages += pages
        yield unigram,(int(partial_count),int(partial_pages))

    def reducer(self,unigram,duple):
        # Aggregate results
        total_count = 0
        total_pages = 0
        for count,pages in duple:
            total_count += count
            total_pages += pages
        # Calculate density (minimum value will be 1.0)
        density = float(total_count)/total_pages
        yield density,unigram
        
if __name__ == '__main__':
    Density.run()