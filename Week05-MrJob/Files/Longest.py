#!/home/hduser/anaconda/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

class Longest(MRJob):
    
    # Define a global variable that captures the longest n-gram found 'til now
    longest = 0 
    
    def jobconf(self):
        orig_jobconf = super(Longest, self).jobconf()        
        custom_jobconf = {
            'mapred.output.key.comparator.class': 
                'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
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
        length = len(ngram)-4 # Lenght of the n-gram excluding (n-1) spaces
        # Only yield results if current n-gram is equal or longer than previous 
            # ones
        # This part is optional, but dramatically reduces the mappers' outputs
        if length >= self.longest:
            self.longest = length
            yield int(length),ngram
    
    def reducer(self,length,values):
        # Again, compare with previous n-grams
        if int(length) >= self.longest:
            self.longest = int(length)
            for ngram in values:
                yield length, ngram
        
if __name__ == '__main__':
    Longest.run()