import time
from numpy import *

while True:
	time.sleep(5)
	ran='['+str(round(random.uniform(0,1),1))+','+str(round(random.uniform(0,1),1))+','+str(round(random.uniform(2,3),1))+']'+'\n'
	with open('streamapp/actual/train.txt','a+') as f: f.write(str(ran))