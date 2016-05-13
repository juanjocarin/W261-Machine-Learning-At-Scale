#!/home/hduser/anaconda/bin/python
from HW541_TopN import HW541_TopN

import os

mr_job = HW541_TopN(args=[
        's3://filtered-5grams/', '-r', 'emr', 
        '--number_unigrams=10000',
        '--output-dir=s3://ucb-mids-mls-juanjocarin/Top10k_output',
        '--no-output'])

with mr_job.make_runner() as runner: 
    runner.run()

os.system("aws s3 cp s3://ucb-mids-mls-juanjocarin/Top10k_output/part-00000 \
    ./Top10k.txt")
os.system("aws s3 rm s3://ucb-mids-mls-juanjocarin/Top10k_output/part-00000")
os.system("aws s3 rm s3://ucb-mids-mls-juanjocarin/Top10k_output/_SUCCESS")