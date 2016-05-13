#!/home/hduser/anaconda/bin/python
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.protocol import RawValueProtocol
from mrjob.compat import get_jobconf_value
import ast
from math import log1p, exp, log

GLOBAL_PATH = "/HD/Dropbox2/Dropbox/W261/HW9/"

## A function to sum log probabilities
def sum_log(p, q):
    if q > p:
        b = log(q)
        if p == 0:
            return exp(b + log1p(0))
        else:
            a = log(p)
            return exp(b + log1p(exp(a-b)))
    else:
        if q == 0:
            if p == 0:
                return 0.
            else:
                a = log(p)
                return exp(a + log1p(0))
        else:
            b = log(q)
            a = log(p)
            return exp(a + log1p(exp(b-a)))
            
class PageRank(MRJob):
    
    #OUTPUT_PROTOCOL = RawValueProtocol

    alpha = 0.85

    def steps(self):
        return [
            MRStep(mapper_init = self.mapper1_init,
                   mapper = self.mapper1, 
                   mapper_final = self.mapper1_final, 
                   reducer_init = self.reducer1_init, 
                   reducer = self.reducer1, 
                   reducer_final = self.reducer1_final),
            MRStep(mapper_init = self.mapper2_init,
                   mapper = self.mapper2, 
                   mapper_final = self.mapper2_final, 
                   reducer = self.reducer2)
        ]

    def mapper1_init(self):
        ## Associative array for in-mapper combining
        self.nodes = {}
        self.nodes_prev = {}
        ## Keep track of nodes in the graph
        self.list_nodes = []
        self.list_sources = []
    
    def mapper1(self, _, line):
        ## Format of each line:
            ## n1 TAB {n2: 1, n3:1, ...} TAB PR(n1)
                ## The 3rd field is not present in the 1st iteration
                ## which only contains each node and its adjacency list
        #line = line.strip().split('\t')
        ## 1st field is a node that acts as source
            ## (Though from 2nd iteration on all nodes will be present,
            ## some with an empty list of outgoing links)
        #source = line[0]
        ## 2nd field is a dictionary of links with their weights
            ## (Set to 1 from 2nd iteration on; they're not relevant anyway)
        #sink = ast.literal_eval(line[1])
        ## Keep only the sinks, not the weights
        #sinks = sink.keys()
        ## Include those sinks in a list of nodes
        
        line = line.strip().split('\t')
        source = line[0].strip('"')
        if isinstance(ast.literal_eval(line[1]), dict):
            sinks = ast.literal_eval(line[1]).keys()
            if len(line) < 3:
                PR = 1e-3
                for sink in sinks:
                    self.nodes_prev[sink] = PR
            else:
                PR = float(line[2])
                self.nodes_prev[source] = PR
        else:
            sinks = ast.literal_eval(line[1])[0].keys()
            PR = float(ast.literal_eval(line[1])[1])
            self.nodes_prev[source] = PR

        for sink in sinks:
            if sink not in self.list_nodes:
                self.list_nodes.append(sink)

        ## Pass the graph structure (the adjacency list)
        yield source, [sinks]
        
        ## Include the source in the list of nodes, too
        if source not in self.list_nodes:
            self.list_nodes.append(source)
        # And also in a list of sources... if it really has outgoing links!
        if source not in self.list_sources and len(sinks) != 0:
            self.list_sources.append(source)
        ## If PR of the source is not present (1st iteration)
        #if len(line) < 3:
            ## All nodes have an initial PR of 1e-3
                ## The value can be any (not necessarily 1/|G|)
                    ## (We don't know the value of |G| yet!!!)
                ## Just takes more or less to converge
                    ## and the sum of PRs in the first iterations
                    ## will be less than 100%
        #    PR = 1e-3
            ## Keep track of the previous PR to distribute the PR mass
                ## of the dangling nodes
                ## (They are never sources, but have to be sinks)
        #    for sink in sinks:
        #        self.nodes_prev[sink] = PR    
        #else:
            ## From 2nd iteration on, we already know the PR
        #    PR = float(line[2])
        #    self.nodes_prev[source] = PR
        
        ## Distribute the mass of the source along its sinks
            ## We put the value in the associative array
            ## and emit it in the in-mapper combiner
        for node in sinks:
            self.nodes.setdefault(node,0.)
            self.nodes[node] = sum_log(self.nodes[node], PR/len(sinks))
    
    def mapper1_final(self):
        ## For all nodes detected
        for node in self.list_nodes:
            ## If they have ingoing links, emit their PR
                ## as well as the total number of nodes (|G|)
            if node in self.nodes.keys():
                yield node, [self.nodes[node], len(self.list_nodes), 0.]
            ## If not a source (i.e., a DANGLING NODE) emit its previos PR
                ## to be distributed; otherwise, 0
            if node in self.nodes_prev.keys():
                yield node, [0., len(self.list_nodes), self.nodes_prev[node]]
            ## If not (they are sources but not sinks), their PR will be 0
                ## (before considering dangling nodes & teleportation)
            else:
                yield node, [0., len(self.list_nodes), 0.]
                
        ## 1st mapper emits each node as key, and 3 values per node
            ## Current PR (w/o considering dangling nodes and teleportation yet)
            ## |G|: number of nodes in the graph
            ## Previous PR if node is a dangling one, 0 otherwise
        ## Also (part of) the structure of the graph (adjacency lists)
    
    def reducer1_init(self):
        ## Keep track of nodes in the graph
        self.nodes = {}
        self.dangling_nodes_mass = 0
        
    def reducer1(self, key, value):
        ## Variables to keep track / aggregate PRs, number of nodes, sinks, etc.
        PR = 0.
        num_nodes = 0
        sinks = {}
        outlinks = []
        
        if key not in self.nodes.keys():
            self.nodes.setdefault(key, [])
            
        prev_mass = 0.
        for v in value:
            node_type = 'sink'
            ## When the value is the graph structure (outlinks of a node)
            if isinstance(v[0], list):
                outlinks = v[0]
                if len(outlinks) != 0:
                    node_type = 'source'
            ## When the value is the mass passed by a neighbor linking to the 
                ## node (as well as the number of nodes and previous PR in 
                ## case of a dangling node)
            else:
                PR = sum_log(PR, v[0])
                # num_nodes = v[1]
                prev_mass = sum_log(prev_mass, v[2])
        if node_type == 'sink':
            self.dangling_nodes_mass = sum_log(self.dangling_nodes_mass, prev_mass)
        
        ## Add weights to the adjacency list to be consistent with the original
            ## file structure
        for node in outlinks:
            sinks[node] = 1
            
        ## The 1st job emits each node as key, and 4 values
            ## its adjacency list
            ## its current PageRank
            ## the total number of nodes found in the graph
            ## the previous PR in case of a dangling node
        self.nodes[key] = [sinks, PR, num_nodes]
        
    def reducer1_final(self):
        num_nodes = len(self.nodes.keys())
        with open(GLOBAL_PATH + 'num_mass.txt', 'a') as f:
                f.writelines(str(num_nodes) + '\t' + str(self.dangling_nodes_mass) + '\n')
        for k, v in self.nodes.iteritems():
            yield k, [v[0], v[1]]
            
    
    #############
    ## 2nd JOB ##
    #############
    
    def mapper2_init(self):
        ## Associative array for in-mapper combining
        self.nodes = {}
        ## Variable to keep track of the "lost" mass
        #self.dangling_nodes_mass = 0
        
    def mapper2(self, key, value):
        ## Aggregate (previous!) PR mass of the dangling nodes
        #self.dangling_nodes_mass = sum_log(self.dangling_nodes_mass, value[3])
        ## Don't need the 4th value anymore
        #self.nodes[key] = value[:3]
        yield key, value
    
    def mapper2_final(self):
        ## Emit the associative array
        for node in self.nodes.keys():
            value = self.nodes[node]
            ## Include the PR mass of dangling nodes again
                ## But now it's the total mass, and included in every node
            #value.append(self.dangling_nodes_mass)
            #yield node, value
        
    def reducer2(self, key, value):
        mass = 0.
        num_nodes = 0
        with open(GLOBAL_PATH + 'num_mass.txt', 'r') as f:
            for line in f:
                line = line.strip().split('\t')
                num_nodes = num_nodes + int(line[0])
                mass = mass + float(line[1])
        for v in value:    
            ## 1) Its adjacency list
            sinks = v[0]
            ## 2) The corrected PR
                ## PR = alpha * PR + alpha * m / |G| + (1-alpha) * (1/|G|)
            PR = self.alpha*sum_log(v[1], mass/num_nodes)
            PR = sum_log(PR, (1-self.alpha)/num_nodes)
        yield key, [sinks, PR]
        
if __name__ == '__main__':
    PageRank.run()