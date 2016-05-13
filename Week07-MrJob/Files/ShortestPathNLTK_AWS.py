#!/home/hduser/anaconda/bin/python
from numpy import argmin, array, random
from mrjob.job import MRJob
from mrjob.job import MRStep
import ast
#from itertools import combinations
#from mrjob.protocol import RawValueProtocol

GLOBAL_PATH = "/HD/Dropbox2/Dropbox/W261/HW7/"

class ShortestPathNLTK_AWS(MRJob):
#    Frontiers = []
#    SSSP = {}

#    OUTPUT_PROTOCOL = RawValueProtocol
    
#    def jobconf(self):
#        orig_jobconf = super(ShortestPathNLTK_AWS, self).jobconf()        
#        custom_jobconf = {
#            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
#            'mapred.text.key.comparator.options': '-k1rn',
#        }
#        combined_jobconf = orig_jobconf
#        combined_jobconf.update(custom_jobconf)
#        self.jobconf = combined_jobconf
#        return combined_jobconf
        
    
    def steps(self):
        return [
            MRStep(mapper_init = self.mapper_init, 
                   mapper=self.mapper,
                   reducer_init = self.reducer_init, 
                   reducer = self.reducer)
        ]

    ## Load Frontiers & SSSP info into Mappers
    def mapper_init(self):
        ## Frontiers is a list of the nodes currenly in the Frontier
        self.SSSP = {}
        self.Frontiers = [s.strip() for s in 
                          open('Frontiers.txt').readlines()]
        open('Frontiers.txt', 'w').close()
        ## SSSP is a dict with the shortest distance and corresponding path for 
            ## each node
        with open('SSSP.txt', 'r') as f:
            for line in f:
                line = line.strip().split('\t')
                dist_path = line[1].split(',')
                self.SSSP[line[0]] = [float(dist_path[0]), dist_path[1]]
        
    ## Load data and output distance & path for nodes connected to those in the
        ## Frontier
    def mapper(self, _, line):
        ## PSEUDOCODE
        ## for each node v in G
            ## for each node u outgouing from v
                ## if v in Frontiers
                    ## Output: (u, SSSP[v] + Dist(v,u))
        line = line.split('\t')
        node = line[0]
        sink = ast.literal_eval(line[1])
        if node in self.Frontiers:
            for sink_node in sink.keys():
                if len(self.SSSP[node][1]) !=0 :
                    yield sink_node, [self.SSSP[node][0] + sink[sink_node], 
                                      self.SSSP[node][1] + '/']
                else:
                    yield sink_node, [self.SSSP[node][0] + sink[sink_node], 
                                      self.SSSP[node][1] + node + '/']
                
    def reducer_init(self):
        ## PSEUDOCODE
        ## Frontiers = []
        self.SSSP = {}
        self.Frontiers = []
        with open('SSSP.txt', 'r') as f:
            for line in f:
                line = line.strip().split('\t')
                dist_path = line[1].split(',')
                self.SSSP[line[0]] = [float(dist_path[0]), dist_path[1]]

    def reducer(self, node, values):
        ## PSEUDOCODE
        ## for each key k find the minimum value Dmin[k]
        ## if Dmin[k] < SSSP[k]
            ## SSSP[k] = Dmin[k]
            ## Push k in Frontiers
            ## Output (k, SSSP[k])
        dist_path = [v for v in values]
        distances = [int(d[0]) for d in dist_path]
        min_distance = min(distances)
        path_min_distance = [d[1] for d in dist_path if d[0]==min_distance][0]
        if min_distance < self.SSSP[node][0]:
            self.SSSP[node][0] = float(min_distance)
            self.SSSP[node][1] = path_min_distance + node
#            self.Frontiers.append(node)
#            Sys.stderr.write(self.SSSP)
            yield node, self.SSSP[node]
#            yield node, self.SSSP[node]
#            out = [node, self.SSSP[node][0], self.SSSP[node][1]]
#            yield None, out
        
if __name__ == '__main__':
    ShortestPathNLTK_AWS.run()