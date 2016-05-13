#Version 1: One MapReduce Stage (join data at the first reducer)
from mrjob.job import MRJob
from mrjob.compat import get_jobconf_value
class MRMatrixAB(MRJob):
    #Emit all the data need to caculate cell i,j in result matrix
    def mapper(self, _, line):
        v = line.split(',')
        n = (len(v)-2)/2 #number of Non-zero columns for this each
        i = int(get_jobconf_value("row.num.A")) # we need to know how many rows of A
        j = int(get_jobconf_value("col.num.B")) # we need to know how many columns of B
        
        if v[0]=='0':
            for p in range(n):
                for q in range(j):
                    yield (int(v[1]),q), (int(v[p*2+2]),float(v[p*2+3]))
            
        elif v[0]=='1':
            for p in range(n):
                for q in range(i):
                    yield (q,int(v[p*2+2])), (int(v[1]),float(v[p*2+3]))
                
    # Sum up the product for cell i,j
    def reducer(self, key, values):
        idx_dict = {}
        s = 0.0
        preidx = -1
        preval = 0
        for idx, value in values:
            if str(idx) in idx_dict:
                s = s + value * idx_dict[str(idx)]
            else:
                idx_dict[str(idx)] = value
        yield key,s

if __name__ == '__main__':
    MRMatrixAB.run()