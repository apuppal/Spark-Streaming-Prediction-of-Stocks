import time
from numpy import *
from random import randint

while True:
	time.sleep(1)
	ran='('+str(randint(0,20))+','+'['+str(round(random.uniform(0,1),1))+','+str(round(random.uniform(0,1),1))+','+str(round(random.uniform(2,3),1))+']'+')'+'\n'
	#change path
	with open('streamapp/actual/test.txt','a+') as f: f.write(str(ran))