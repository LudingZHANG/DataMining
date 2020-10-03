import sys
import pyspark
import time
from pyspark import SparkContext
import collections
from itertools import combinations
import math
import json
from itertools import islice
import operator

def get_avg_rating(rating_list):
	avg_rating = {}
	for r in rating_list:
		if r[0] not in avg_rating:
			avg_rating[r[0]] = [r[1]]
		else:
			avg_rating[r[0]].append(r[1])
	for k in avg_rating:
		avg_rating[k] = sum(avg_rating[k])/len(avg_rating[k])
	return avg_rating

def get_prediction(tup):

	active_user = tup[0]
	active_business = tup[1][0]
	user_rated_business = tup[1][1]

	pairs = []
	for b in user_rated_business:
		if (active_business,b) in model:
			pairs.append((model[(active_business,b)],user_rated_business[b]))
		if (b,active_business) in model:
			pairs.append((model[(b,active_business)],user_rated_business[b]))

	if len(pairs) == 0:
		return 0

	pairs = sorted(pairs, key = operator.itemgetter(0), reverse=True)
	if len(pairs) >= N: 
		pairs = pairs[:3]
	
	num = 0
	den = 0
	rating = 0

	for j in pairs:
		num = num + (j[0] * j[1])
		den = den + j[0]

	if num > 0 and den > 0:
		rating = num/den

	return rating

train_file = sys.argv[1]
test_file = sys.argv[2]
model_file = sys.argv[3]
output_file = sys.argv[4]
cf_type = sys.argv[5]
N = 3

t = time.time()
sc = SparkContext(appName="Filtering")
sc.setLogLevel("ERROR")



if cf_type == "item_based":
	testRDD = sc.textFile(test_file).map(lambda s:(json.loads(s)["user_id"],json.loads(s)["business_id"]))
	trainRDD = sc.textFile(train_file).map(lambda s:(json.loads(s)["user_id"],(json.loads(s)["business_id"],json.loads(s)["stars"]))) \
									  .groupByKey() \
									  .mapValues(list) \
									  .map(lambda x:(x[0],get_avg_rating(x[1])))
	model = {}
	with open(model_file,"r") as fp:
		for line in fp:
			d = json.loads(line)
			model[(d["b1"],d["b2"])] = d["sim"]

	testRDD = testRDD.join(trainRDD).map(lambda s:(s[0],s[1][0],get_prediction(s))).filter(lambda s:s[2] != 0).collect()
	
	with open(output_file,"w") as fp:
		for t in testRDD:
			json.dump({"user_id":t[0],"business_id":t[1],"stars":t[2]},fp)
			fp.write("\n")


elif cf_type == "user_based":
	testRDD = sc.textFile(test_file).map(lambda s:(json.loads(s)["business_id"],json.loads(s)["user_id"]))
	trainRDD = sc.textFile(train_file).map(lambda s:(json.loads(s)["business_id"],(json.loads(s)["user_id"],json.loads(s)["stars"]))) \
									  .groupByKey() \
									  .mapValues(list) \
									  .map(lambda x:(x[0],get_avg_rating(x[1])))
	model = {}
	with open(model_file,"r") as fp:
		for line in fp:
			d = json.loads(line)
			model[(d["u1"],d["u2"])] = d["sim"]

	testRDD = testRDD.join(trainRDD).map(lambda s:(s[0],s[1][0],get_prediction(s))).filter(lambda s:s[2] != 0).collect()		
	with open(output_file,"w") as fp:
		for t1 in testRDD:
			json.dump({"user_id":t1[0],"business_id":t1[1],"stars":t1[2]},fp)
			fp.write("\n")

else:
	print("Invalid Argument")
print("Duration :- ",time.time()-t)