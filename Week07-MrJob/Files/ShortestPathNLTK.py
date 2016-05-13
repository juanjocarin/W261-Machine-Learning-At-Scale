from numpy import argmin, array, random
from mrjob.job import MRJob
from mrjob.job import MRStep
import ast

GLOBAL_PATH = "/HD/Dropbox2/Dropbox/W261/HW7/"

class ShortestPathNLTK(MRJob):
    Frontiers = []
    SSSP = {}
    
    def steps(self):
        return [
            MRStep(mapper_init = self.mapper_init, 
                   mapper=self.mapper,
                   reducer_init = self.reducer_init, 
                   reducer = self.reducer
                  )
        ]

    #load Frontiers & SSSP info from file
    def mapper_init(self):
        self.Frontiers = [s.strip() for s in 
                          open(GLOBAL_PATH + 'Frontiers.txt').readlines()]
        open(GLOBAL_PATH + 'Frontiers.txt', 'w').close()
        with open(GLOBAL_PATH + 'SSSP.txt', 'r') as f:
            for line in f:
                line = line.strip().split('\t')
                dist_path = line[1].split(',')
                self.SSSP[line[0]] = [float(dist_path[0]), dist_path[1]]
        
    # For each node in the frontier, output 
    def mapper(self, _, line):
        line = line.split('\t')
        node = line[0]
        sink = ast.literal_eval(line[1])
        if node in self.Frontiers:
            for sink_node in sink.keys():
                # Print : Node [Distance, Path]
                if len(self.SSSP[node][1]) !=0 :
                    yield sink_node, [self.SSSP[node][0] + sink[sink_node], self.SSSP[node][1] + '/']
                else:
                    yield sink_node, [self.SSSP[node][0] + sink[sink_node], self.SSSP[node][1] + node + '/']
                
    # Load SSSP file into reducer
    def reducer_init(self):
        self.Frontiers = []
        with open(GLOBAL_PATH + 'SSSP.txt', 'r') as f:
            for line in f:
                line = line.strip().split('\t')
                dist_path = line[1].split(',')
                self.SSSP[line[0]] = [float(dist_path[0]), dist_path[1]]

    def reducer(self, node, values):
        dist_path = [v for v in values]
        distances = [int(d[0]) for d in dist_path]
        min_distance = min(distances)
        path_min_distance = [d[1] for d in dist_path if d[0]==min_distance][0]
        if min_distance < self.SSSP[node][0]:
            self.SSSP[node][0] = float(min_distance)
            self.SSSP[node][1] = path_min_distance + node
            with open(GLOBAL_PATH + 'SSSP.txt', 'w+') as f:
                for k in self.SSSP.keys():
                    f.writelines(k + '\t' + ','.join([str(x) for x in self.SSSP[k]]) + '\n')
                self.Frontiers.append(node)
            with open(GLOBAL_PATH + 'Frontiers.txt', 'w+') as f:
                for node in self.Frontiers:
                    f.writelines(node + '\n')
            yield None, node
        
if __name__ == '__main__':
    ShortestPathNLTK.run()