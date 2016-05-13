#!/home/hduser/anaconda/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from itertools import combinations

class sortingExample(MRJob):
    def jobconf(self):
        orig_jobconf = super(sortingExample, self).jobconf()        
        custom_jobconf = {
            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapred.text.key.comparator.options': '-k1rn',
        }
        combined_jobconf = orig_jobconf
        combined_jobconf.update(custom_jobconf)
        self.jobconf = combined_jobconf
        return combined_jobconf
    def steps(self):
        return [MRStep(mapper = self.mapper, reducer = self.reducer)]
    def mapper(self, _, line):
        line.strip()
        [ngram,count,pages,books] = re.split("\t",line)
        yield int(count),ngram
    def reducer(self,count,values):
        for ngram in values:
            yield ngram,count
    
if __name__ == '__main__':
    sortingExample.run()