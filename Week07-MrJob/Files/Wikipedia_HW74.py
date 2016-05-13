#!/home/hduser/anaconda/bin/python
from ShortestPath import ShortestPath
current_file = 's3://juanjocarin-w261/HW7/directed_toy.txt'
mr_job = ShortestPath(args=[current_file])

import os

nodes = set()
with open('s3://juanjocarin-w261/HW7/directed_toy.txt', 'r') as myfile:
    for line in myfile:
        line = line.split('\t')
        node = line[0]
        if node not in nodes:
            nodes.add(node)
        node_neighbors = ast.literal_eval(line[1])
        for k in node_neighbors.keys():
            if k not in nodes:
                nodes.add(k)
nodes = list(nodes)
    ## Define source
source = '6176135'
    ## Initialize SSSP
SSSP = {}
for node in nodes:
    if node == source:
        SSSP[node] = [0, ""]
    else:
        SSSP[node] = [float('inf'), ""]
with open(GLOBAL_PATH + 'SSSP.txt', 'w+') as f:
    for node in SSSP.keys():
        f.writelines(node + '\t' + ','.join([str(x) for x in SSSP[node]]) + '\n')
## Initialize Frontier
Frontiers = [source]
with open(GLOBAL_PATH + 'Frontiers.txt', 'w+') as f:
    for node in Frontiers:
        f.writelines(node + '\n')


mr_job = HW541(args=['s3://juanjocarin-w261/HW7directed_toy.txt', '-r', 'emr',
                     '--pool-emr-job-flows',
                     '--output-dir=s3://juanjocarin-w261/HW7/Wikipedia',
                     '--no-output'])

with mr_job.make_runner() as runner: 
    runner.run()


print '==== ', current_file, ' ===='

# Update shortest paths iteratively
i=0
stop_condition = False
while len(Frontiers) != 0 and stop_condition == False:
    Frontiers = []
    SSSP = {}
    with open('s3://juanjocarin-w261/HW7/SSSP.txt', 'r') as f:
        for line in f:
            line = line.strip().split('\t')
            dist_path = line[1].split(',')
            SSSP[line[0]] = [float(dist_path[0]), dist_path[1]]
    print "\nIteration "+str(i+1)+":"
    with mr_job.make_runner() as runner: 
        runner.run()
        # stream_output: get access of the output 
        for line in runner.stream_output():
            key,value =  mr_job.parse_output_line(line)
            if key == source:
                stop_condition == True
            Frontiers.append(key)
            SSSP[key] = value
        with open('s3://juanjocarin-w261/HW7/SSSP.txt', 'w+') as f:
            for node in SSSP.keys():
                f.writelines(node + '\t' + ','.join([str(x) for x in SSSP[node]]) + '\n')
    print len(Frontiers)
    with open('s3://juanjocarin-w261/HW7/Frontiers.txt', 'w+') as f:
        for node in Frontiers:
            f.writelines(node + '\n')
    i = i + 1