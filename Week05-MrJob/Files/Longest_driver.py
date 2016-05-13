#!/home/hduser/anaconda/bin/python
from Longest import Longest
mr_job = Longest(args=
                 ['s3://filtered-5grams/',
                  '-r', 'emr'])
with mr_job.make_runner() as runner: 
    runner.run()
    # stream_output: get access of the output 
    for line in runner.stream_output():
        length,ngram = mr_job.parse_output_line(line)
        print str(length) + "\t" + ngram