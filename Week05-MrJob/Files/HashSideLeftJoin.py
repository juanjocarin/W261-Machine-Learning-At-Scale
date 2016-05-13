from mrjob.job import MRJob
import csv
    
class HashSideLeftJoin(MRJob):

    def __init__(self, *args, **kwargs):
        super(HashSideLeftJoin, self).__init__(*args, **kwargs)
        self.TLkeys = []

    def mapper_init(self):
        # Load left-side table in memory as dictionary
        self.TL = {}
        # The absolute path will be passed as argument when calling MRJob
        for key, value in csv.reader(open("TableLeft.txt", "r")):
            # key = webpage ID, value = webpage URL
            self.TL[key] = value   
            self.TLkeys.append(key)
        
    def mapper(self, _, line):
        # Iterate over the right-side table, a record at a time
        TRrecord = line.split(",")
        key = TRrecord[0]
        value_visitor = TRrecord[1]
        # Look for each record, in the left-side table (in-memory)
        if key in self.TL.keys():
            try:
                self.TLkeys.remove(key)
            except ValueError:
                pass
            yield key, (self.TL[key], value_visitor)
    
    def mapper_final(self):
        # Iterate over the right-side table, a record at a time
        for key in self.TLkeys:
            yield key, (self.TL[key], None)
    
    def reducer(self, key, value):
        for val_url, val_visitor in value:
            yield key, (val_url, val_visitor)
            
if __name__ == '__main__':
    HashSideLeftJoin.run()