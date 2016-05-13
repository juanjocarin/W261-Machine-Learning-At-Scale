#!/home/hduser/anaconda/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from itertools import combinations

class secondary_sort(MRJob):
    def jobconf(self):
        orig_jobconf = super(secondary_sort, self).jobconf()        
        custom_jobconf = {
            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
            'mapred.text.key.comparator.options': '-k1rn',
            'mapred.tasktracker.reduce.tasks.maximum': '1'
        }
        combined_jobconf = orig_jobconf
        combined_jobconf.update(custom_jobconf)
        self.jobconf = combined_jobconf
        return combined_jobconf
    def steps(self):
        return [MRStep(mapper = self.mapper, reducer = self.reducer)]
    def mapper(self, _, line):
        [word,count] = re.split(",",line)
        yield int(count),word
    def reducer(self,count,values):
        for ngram in values:
            yield int(count),ngram
    
if __name__ == '__main__':
    secondary_sort.run()