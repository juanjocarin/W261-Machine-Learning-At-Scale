#Version 1: One MapReduce Stage (join data at the first reducer)
from mrjob.job import MRJob

class MRMatrixX2(MRJob):
    #Emit all the data need to caculate cell i,j in result matrix
    def mapper(self, _, line):
        v = line.split(',')
        # add 1s to calculate intercept
        v.append('1.0')
        for i in range(len(v)-2):
            for j in range(len(v)-2):
                yield (j,i),(int(v[0]),float(v[i+2]))
                yield (i,j),(int(v[0]),float(v[i+2]))
                
    # Sum up the product for cell i,j
    def reducer(self, key, values):
        idxdict = {}
        s = 0.0
        preidx = -1
        preval = 0
        f = []
        for idx, value in values:
            if str(idx) in idxdict:
                s = s + value * idxdict[str(idx)]
            else:
                idxdict[str(idx)] = value
        yield key,s

if __name__ == '__main__':
    MRMatrixX2.run()