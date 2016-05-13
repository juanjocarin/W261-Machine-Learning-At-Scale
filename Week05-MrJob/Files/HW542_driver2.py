#!/home/hduser/anaconda/bin/python
from HW542 import HW542

import os

mr_job = HW542(args=['~/Downloads/stripechunkaa', 
                     '-r', 'emr'])
with mr_job.make_runner() as runner:
    runner.run()