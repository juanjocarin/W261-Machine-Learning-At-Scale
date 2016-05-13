#!/home/hduser/anaconda/bin/python
from Density import Density
import os
mr_job = Density(args=[
        's3://filtered-5grams/','-r', 'emr',
        '--output-dir=s3://ucb-mids-mls-juanjocarin/Density_output',
        '--no-output'
    ])

with mr_job.make_runner() as runner: 
    runner.run()
os.system("aws s3 cp s3://ucb-mids-mls-juanjocarin/Density_output/part-00000 \
    s3://ucb-mids-mls-juanjocarin/DenseUnigrams.txt")
os.system("aws s3 rm s3://ucb-mids-mls-juanjocarin/Density_output/part-00000")
os.system("aws s3 rm s3://ucb-mids-mls-juanjocarin/Density_output/_SUCCESS")