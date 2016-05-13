#!/home/hduser/anaconda/bin/python
from HW541 import HW541
#from HW542 import HW542

import os

mr_job = HW541(args=['s3://filtered-5grams/', '-r', 'emr',
                     '--output-dir=s3://ucb-mids-mls-juanjocarin/CoOccurrence',
                     '--no-output'])
with mr_job.make_runner() as runner: 
    runner.run()
os.system("aws s3 cp s3://ucb-mids-mls-juanjocarin/CoOccurrence/part-00000 \
    s3://ucb-mids-mls-juanjocarin/CoOccurrence_stripes.txt")
os.system("aws s3 rm s3://ucb-mids-mls-juanjocarin/CoOccurrence/part-00000")
os.system("aws s3 rm s3://ucb-mids-mls-juanjocarin/CoOccurrence/_SUCCESS")

#mr_job = HW542(args=['s3://ucb-mids-mls-juanjocarin/CoOccurrence_stripes.txt', 
#                     '-r', 'emr',
#                     '--output-dir=s3://ucb-mids-mls-juanjocarin/Comparison',
#                     '--no-output'])
#with mr_job.make_runner() as runner:
#    runner.run()
#os.system("aws s3 cp s3://ucb-mids-mls-juanjocarin/Comparison/part-00000 \
#    s3://ucb-mids-mls-juanjocarin/Stripe_commparison.txt")
#os.system("aws s3 rm s3://ucb-mids-mls-juanjocarin/Comparison/part-00000")
#os.system("aws s3 rm s3://ucb-mids-mls-juanjocarin/Comparison/_SUCCESS")    