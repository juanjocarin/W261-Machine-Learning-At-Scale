#!/home/hduser/anaconda/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from itertools import combinations 
from operator import itemgetter
from math import sqrt
from mrjob.protocol import RawValueProtocol

class HW542(MRJob):

    def jobconf(self):
        orig_jobconf = super(HW542, self).jobconf()        
        custom_jobconf = {'mapred.reduce.tasks': '1',
                          'mapred.output.key.comparator.class': 
                          'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                          'mapred.text.key.comparator.options': '-k1n'}
        combined_jobconf = orig_jobconf
        combined_jobconf.update(custom_jobconf)
        self.jobconf = combined_jobconf
        return combined_jobconf
    
    
    OUTPUT_PROTOCOL = RawValueProtocol

    def steps(self):
        return [MRStep(
                mapper = self.mapper, 
                reducer = self.reducer)]
    #,
    #            MRStep(
    #            reducer = self.reducer_aggregation)]

    
    def mapper(self, _, line):
        # i-th line (corresponding to i-th unigram from the top 10,000 
            # frequent contains the i-th coordinates for all unigrams
        line = re.sub('\"', '', line)
        line = line.split()
        unigram = line[0]
        coords = line[1].split(',')
        N = len(coords)
        # We have N (=10,000) coordinates and points
        # For each row (or vector) of N elements we're going to calculate N
            # other vectors, by subtracting the 1st, second, ... N-th element
            # and taking the absolute value
        # We also need the unigram, because (since they were ordered 
            # alphabetically) it will allow us to detect the value of "i"
        for i in range(len(coords)):
            yield i, (unigram,[abs(int(coords[i])-int(x)) for x in coords])
    
    def reducer(self, row, values):
        unigram = []
        sum_coord = None
        for value in values:
            N = len(value[1])
            if not sum_coord:
                sum_coord = [0]*N
            unigram.append(value[0])
            #sum_coord += [s+int(v) for s,v in zip(sum_coord,value)]
            sum_coord = [s+int(v) for s,v in zip(sum_coord,value[1])]
        yield None,sorted(unigram)[row]+','+','.join([str(x) for x in sum_coord])
        
if __name__ == '__main__':
    HW542.run()