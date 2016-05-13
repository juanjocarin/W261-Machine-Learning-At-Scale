#!/usr/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol,JSONProtocol
import re
from itertools import combinations

class correlateCoOccurrences(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol    
    N = 10000
    
    ## a custom jobconf for the final reduce, numeric sort step
    sortconf = {
        'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
        'mapred.text.key.comparator.options': '-k1,1',
        'mapred.reduce.tasks': '1',
    }
    
    ## three steps here
    def steps(self):
        return [MRStep(
                mapper = self.mapper, 
                reducer = self.reducer_differences
            ),
                MRStep(
                mapper_init = self.mapper_init_aggregation,
                reducer_init = self.reducer_init_aggregation,
                reducer = self.reducer_aggregation
            ),
                MRStep(
                mapper_init = self.mapper_init_sort,
                reducer = self.reducer_sort,
                jobconf = self.sortconf
            )]
    
    ## Step 1 mapper: ranks the data, and transposes by using the column (i) as key
    def mapper(self, _, line):
        data = re.split(",",line)
        word = data[0]
        counts = [int(data[i+1]) for i in range(len(data)-1)]
        ranks = sorted(range(len(counts)), key=lambda k: counts[k])
        for i in range(len(ranks)):
            rank = ranks[i]
            yield str(i).zfill(5),[word,rank]
            
    ## Step 1 reducer: collects the ranks for a column, 
    ## and computed N choose 2 differences, 
    ## yielding the differences in lists for those pairs having
    ## the same common first word
    def reducer_differences(self,colNum,values):
        corNum = 0
        ranks = []
        words = []
        for value in values:
            word,rank = value
            ranks.append(rank)
            words.append(word)
        sortIDX = sorted(range(self.N), key=lambda k: words[k])
        for i in range(self.N-1):
            diffs = []
            for j in range(self.N):
                if j > i:
                    diff = (float(ranks[sortIDX[i]]) - float(ranks[sortIDX[j]]))**2
                    diffs.append(diff)
                    corNum += 1
            yield i,diffs
    
    ## Step 2 mapper init: we have set the input protocol to JSON 
    ## for the (default, unspecified) identity mapper!
    def mapper_init_aggregation(self):
        self.INPUT_PROTOCOL = JSONProtocol
    
    ## Step 2 reducer init: It will help in our reducer to have the 
    ## alphabetically sorted list of 10,000 words
    def reducer_init_aggregation(self):
        self.words = []
        f = open("top10kWords.txt","r")
        for word in f:
            word = word.strip()
            self.words.append(word)
    
    ## Step 2 reducer: This reducer is responsible for aggregating 
    ## the differences for each correlation, computing the final
    ## correlation values, and yielding them (as keys) with the word,word pairs.
    def reducer_aggregation(self,i,values):
        correlations = []
        for j in range(self.N):
            if j > i:
                correlations.append(0)
        for diffs in values:
            correlations = [correlations[j] + diffs[j] for j in range(len(correlations))]
        for j in range(len(correlations)):
            correlation = 1-(6*correlations[j]/float(self.N*(self.N**2 - 1)))
            pair = self.words[i]+","+self.words[j+i+1]
            yield correlation,pair
            
    ## Step 3 mapper init: we have set the input protocol to JSON 
    ## for the (default, unspecified) identity mapper!
    def mapper_init_sort(self):
        self.INPUT_PROTOCOL = JSONProtocol

    ## Step 3 reducer: all we're doing is yielding our 10,000 choose 2
    ## sorted correlations, with None as key, since we are using the RawValueProtocol
    def reducer_sort(self,correlation,pairs):
        for pair in pairs:
            yield None,pair+"\t"+str(correlation)

if __name__ == '__main__':
    correlateCoOccurrences.run()