from pyspark import SparkContext, SparkConf
import sys
import json
import os
from operator import add

review_file = sys.argv[1]
business_file = sys.argv[2]
output_file = sys.argv[3]
if_spark = sys.argv[4]
top_n_categories = int(sys.argv[5])


if if_spark == "spark":

	conf = SparkConf().setAppName("inf553")
	sc = SparkContext(conf=conf)
	sc.setLogLevel("OFF")
	reviewRDD = sc.textFile(review_file)
	businessRDD = sc.textFile(business_file)	

	categories = businessRDD.map(lambda s: (json.loads(s)["business_id"],json.loads(s)["categories"])).filter(lambda s: s[1] is not None).map(lambda s: (s[0],tuple([x.strip() for x in s[1].split(",")])))
	reviews = reviewRDD.map(lambda s: (json.loads(s)["business_id"],json.loads(s)["stars"]))
	sum_count = reviews.join(categories).flatMap(lambda s: [(value, s[1][0]) for value in s[1][1]]).aggregateByKey((0,0), lambda U,v: (U[0] + v, U[1] + 1), lambda U1,U2: (U1[0] + U2[0], U1[1] + U2[1]))
	average = sum_count.map(lambda s : (s[0], float(s[1][0])/s[1][1]))
	top_categories = average.sortBy(lambda x: (-x[1],x[0])).map(lambda s: [s[0],s[1]]).take(top_n_categories)
	
	result = {}
	result["result"] = top_categories
	with open(output_file,"w") as f:
		json.dump(result,f)	

elif if_spark == "no_spark":

	all_categories = {}
	all_reviews = []
	
	with open(review_file,"r") as f1 :
		for line in f1:
			r = json.loads(line)
			all_reviews.append({r["business_id"]:r["stars"]})
			
	with open(business_file,"r") as f2:			
		for line in f2:
			b = json.loads(line)
			if b["categories"] is not None:
				all_categories[b["business_id"]]=[x.strip() for x in b["categories"].split(",")]

	joined = []
	for i in all_reviews: 
		key = list(i.keys())[0]
		val = list(i.values())[0]
		if key in all_categories.keys(): 
			joined.append([val,all_categories[key]])

	distinct = {}
	for idx in joined:
		val = idx[0]
		for key in idx[1]: 
			if key not in distinct.keys():
				summed = val
				count = 1
				distinct[key] = [summed,count] 
			else:
				distinct[key][0] += val
				distinct[key][1] += 1

	output = {} 
	for key,val in distinct.items(): 
		output[key]=val[0]/val[1]


	result = {}
	result["result"] = [list(x) for x in sorted(output.items(),key=lambda item :(-item[1], item[0]))][0:top_n_categories]	
	with open(output_file,"w") as f:
		json.dump(result,f)	

else:
	print("Invalid argument")
	sys.exit(1)

