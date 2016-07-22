#To generate test data
import time
from numpy import *

while True:
	time.sleep(1)
	ran=str(str(round(random.uniform(0,20),1))+' '+str(round(random.uniform(0,20),1))+' '+str(round(random.uniform(0,20),1))+'\n')
	with open('/streamapp/working/test.txt','a+') as f: f.write(str(ran))

#To generate train data if needed

while True:
	time.sleep(1)
	ran=str(str(round(random.uniform(0,20),1))+' '+str(round(random.uniform(0,20),1))+' '+str(round(random.uniform(0,20),1))+'\n')
	with open('/streamapp/working/train.txt','a+') as f: f.write(str(ran))
