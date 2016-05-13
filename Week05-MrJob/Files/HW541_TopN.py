#!/home/hduser/anaconda/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
import re
from operator import itemgetter
from mrjob.compat import get_jobconf_value

class HW541_TopN(MRJob):
    
    OUTPUT_PROTOCOL = RawValueProtocol
    
    def jobconf(self):
        orig_jobconf = super(HW541_TopN, self).jobconf()        
        custom_jobconf = {'mapred.reduce.tasks': '1'}
        combined_jobconf = orig_jobconf
        combined_jobconf.update(custom_jobconf)
        self.jobconf = combined_jobconf
        return combined_jobconf
    
    def configure_options(self):
        super(HW541_TopN, self).configure_options()
        # The number of most frequent unigrams can be configured by
            # the user as an argument
        self.add_passthrough_option('--number_unigrams',  
                                    dest='number_unigrams', type='int', 
                                    default=10)
    
    def steps(self):
        return [MRStep(mapper = self.mapper, combiner = self.combiner,
                       reducer_init = self.reducer_init, 
                       reducer = self.reducer, 
                       reducer_final = self.reducer_final)]
    
    def mapper(self, _, line):
        line.strip()
        [ngram,count,pages,books] = re.split("\t",line)
        # Output the count for each word in the 5-gram
        unigrams = ngram.split()
        for unigram in unigrams:
            yield unigram, int(count)

    def combiner(self, unigram, count):
        yield unigram, sum(count)

    def reducer_init(self):
        self.top = {}

    def reducer(self, unigram, count):
        total = sum(count)
        # If we have not exceeded max size of the dictionary yet
        if len(self.top.keys()) < self.options.number_unigrams:
            self.top[unigram] = total
        # If exceeded, include new unigram only if more frequent that
                # other previously stored
        else:
            if total > min(self.top.values()):
                # Remove unigram not so frequent
                self.top.pop(min(self.top, key = self.top.get))
                # Add new unigram
                self.top[unigram] = total
    
    def reducer_final(self):
        for unigram in self.top.keys():
            yield None,unigram+'\t'+str(self.top[unigram])

if __name__ == '__main__':
    HW541_TopN.run()