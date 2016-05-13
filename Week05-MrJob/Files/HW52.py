#!/home/hduser/anaconda/bin/python
import sys
JoinType = sys.argv[1]

# Import the class

if JoinType == 'Inner':
    from HashSideInnerJoin import HashSideInnerJoin
    JoinClass = 'HashSideInnerJoin'
    output = 'InnerJoinTable.txt'
elif JoinType == 'Right':
    from HashSideRightJoin import HashSideRightJoin
    JoinClass = 'HashSideRightJoin'
    output = 'RightJoinTable.txt'
elif JoinType == 'Left':
    from HashSideLeftJoin import HashSideLeftJoin
    JoinClass = 'HashSideLeftJoin'
    output = 'LeftJoinTable.txt'
else:
    raise ValueError('USE Inner, Right, OR Left AS ARGUMENTS')
    
# Use the 2 tables, left-side as seconrd argument (to be load by mapper_init)
mr_job = eval(JoinClass)(args=['TableRight.txt', '--file=TableLeft.txt'])
with mr_job.make_runner() as runner:
    runner.run()
    # Create the join table
    with open(output,'w') as result:
        for line in runner.stream_output():
            webpageID = str(mr_job.parse_output_line(line)[0])
            # Extract webpage URL and visitor ID from value
            webpageURL = mr_job.parse_output_line(line)[1][0]
            visitorID = str(mr_job.parse_output_line(line)[1][1])
            result.writelines(webpageID + ',' + webpageURL + ',' + visitorID 
                              +'\n')
    result.close()