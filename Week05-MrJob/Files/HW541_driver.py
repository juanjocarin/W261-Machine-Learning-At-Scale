#!/home/hduser/anaconda/bin/python
from HW541 import HW541

import os

mr_job = HW541(args=['s3://filtered-5grams/', '-r', 'emr',
                     '--file=Top10kWords.txt',
                     '--output-dir=s3://ucb-mids-mls-juanjocarin/Stripes_output',
                     '--no-output'])

with mr_job.make_runner() as runner: 
    runner.run()

os.system("aws s3 cp s3://ucb-mids-mls-juanjocarin/Stripes_output/part-00000 \
    s3://ucb-mids-mls-juanjocarin/Stripes.txt")
os.system("aws s3 rm s3://ucb-mids-mls-juanjocarin/Stripes_output/part-00000")
os.system("aws s3 rm s3://ucb-mids-mls-juanjocarin/Stripes_output/_SUCCESS")