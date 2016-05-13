#!/home/hduser/anaconda/bin/python
from HW542 import HW542

import os

mr_job = HW542(args=['s3://ucb-mids-mls-juanjocarin/Stripes_chunks/stripechunk_aa', 
                     '-r', 'emr',
                     '--output-dir=s3://ucb-mids-mls-juanjocarin/Manhattan_output2',
                     '--no-output'])
with mr_job.make_runner() as runner:
    runner.run()
#os.system("aws s3 cp s3://ucb-mids-mls-juanjocarin/Manhattan_output/part-00000 \
#    s3://ucb-mids-mls-juanjocarin/Manhattan_distances.txt")
#os.system("aws s3 rm s3://ucb-mids-mls-juanjocarin/Manhattan_output/part-00000")
#os.system("aws s3 rm s3://ucb-mids-mls-juanjocarin/Manhattan_output/_SUCCESS")    