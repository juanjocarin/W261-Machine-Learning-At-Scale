from mrjob.job import MRJob
import csv
    
class HashSideRightJoin(MRJob):

    def mapper_init(self):
        # Load left-side table in memory as dictionary
        self.TL = {}
        # The absolute path will be passed as argument when calling MRJob
        for key, value in csv.reader(open("TableLeft.txt", "r")):
            # key = webpage ID, value = webpage URL
            self.TL[key] = value   
        
    def mapper(self, _, line):
        # Iterate over the right-side table, a record at a time
        TRrecord = line.split(",")
        key = TRrecord[0]
        value_visitor = TRrecord[1]
        # Look for each record, in the left-side table (in-memory)
        if key in self.TL.keys():
            yield key, (self.TL[key], value_visitor)
        # And if there's no match, include the visitor info anyway
        else:
            yield key, (None, value_visitor)
    
    # The reducer is optional. If not specified, I found out records are not 
        # sorted by webpage ID
    def reducer(self, key, value):
        for val_url, val_visitor in value:
            yield key, (val_url, val_visitor)
            
if __name__ == '__main__':
    HashSideRightJoin.run()