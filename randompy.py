while True:
	time.sleep(1)
	ran=str(round(random.uniform(0,20),0))+','+str(round(random.uniform(0,1),1))+','+str(round(random.uniform(0,1),1))+','+str(round(random.uniform(2,3),1))+'\n'
	with open('/streamapp/test.txt','a+') as f: f.write(str(ran))