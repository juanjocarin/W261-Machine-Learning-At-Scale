#!/home/hduser/anaconda/bin/python
from FreqUnigrams import FreqUnigrams
mr_job = FreqUnigrams(args=
                 ['s3://filtered-5grams/',
                  '-r', 'emr'])
with mr_job.make_runner() as runner: 
    runner.run()
    # stream_output: get access of the output 
    for line in runner.stream_output():
        freq,unigram = mr_job.parse_output_line(line)
        print str(freq) + "\t" + unigram