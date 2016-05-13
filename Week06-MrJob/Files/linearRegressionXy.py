from mrjob.job import MRJob

class MRMatrixXY(MRJob):
    def mapper(self, _, line):
        v = line.split(',')
        # product of y*xi
        for i in range(len(v)-2):
            yield i, float(v[1])*float(v[i+2])
        # To calculate Intercept
        yield i+1, float(v[1])
    
    # Sum up the products
    def reducer(self, key, values):
        yield key,sum(values)

if __name__ == '__main__':
    MRMatrixXY.run()