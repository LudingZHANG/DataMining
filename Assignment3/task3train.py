import sys
import pyspark
import time
from pyspark import SparkContext
import collections
from itertools import combinations
import math
import json
from itertools import islice
import itertools

def jaccard_similarity(m):
	a = dataDict[m[0]]
	b = dataDict[m[1]]
	intersection = len(a & b)
	union = len(a) + len(b) - intersection
	return float(intersection)/float(union)


def append(list1,list2):
	list1.append(list2)
	return list1

def hashing(a,b):
	return baskets.map(lambda x : (x[0], ((a * x[1]+ b) % bucket_size))) \
			   .groupByKey().mapValues(list).map(lambda x : (x[0], min(x[1])))

def signature_matrix():
    
	hash1 = hashing(7,11)
	hash2 = hashing(2,13)
	hash3 = hashing(51,58)
	hash4 = hashing(14,38)
	hash5 = hashing(45,98)
	hash6 = hashing(23,29)
	hash7 = hashing(3,2)
	hash8 = hashing(13,19)
	hash9 = hashing(11,17)
	hash10 = hashing(87,109)
	hash11 = hashing(177,83)
	hash12 = hashing(23,73)
	hash13 = hashing(43,87)
	hash14 = hashing(5,7)
	hash15 = hashing(89,22)
	hash16 = hashing(46,42)
	hash17 = hashing(41,51)
	hash18 = hashing(67,43)
	hash19 = hashing(111,82)
	hash20 = hashing(143,41)
	hash21 = hashing(11,14)
	hash22 = hashing(19,23)
	hash23 = hashing(29,67)
	hash24 = hashing(89,111)
	hash25 = hashing(7,19)
	hash26 = hashing(17,23)
	hash27 = hashing(93,87)
	hash28 = hashing(11,72)
	hash29 = hashing(81,9)
	hash30 = hashing(78,24)
	hash31 = hashing(36,12)
	hash32 = hashing(42,24)

	sig_matrix = hash1.join(hash2).mapValues(list)
	sig_matrix = sig_matrix.join(hash3).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash4).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash5).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash6).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash7).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash8).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash9).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash10).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash11).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash12).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash13).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash14).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash15).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash16).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash17).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash18).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash19).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash20).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash21).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash22).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash23).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash24).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash25).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash26).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash27).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash28).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash29).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash30).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash31).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.join(hash32).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	sig_matrix = sig_matrix.sortByKey()
	return sig_matrix


def get_bands(tup):
	bands = []
	u_id = tup[0]
	signatures = tup[1]
	idx = 0
	for i in range(0,B):
		row = []
		for j in range(0,R):
			row.append(signatures[idx])
			idx+=1
		bands.append(((i,tuple(row)),[u_id]))
	return bands

def get_pearson_correlation(r1,r2):
	b1_avg = sum(list(r1.values()))/len(r1)
	b2_avg = sum(list(r2.values()))/len(r2)

	num = 0
	b1_d = 0
	b2_d = 0
	for k,v in r1.items():
		num += (v - b1_avg) * (r2[k] - b2_avg)
		b1_d += math.pow((v - b1_avg),2)
		b2_d += math.pow((v - b2_avg),2)
	den = math.sqrt(b1_d) * math.sqrt(b2_d)
	if num > 0 and den > 0:
		pc = num/den
	else:
		pc = 0
	return pc


def take(n, iterable):
	"Return first n items of the iterable as a list"
	return list(islice(iterable, n))


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

def get_correlation(pair):
	u_b1 = grouped[pair[0]]								# users who rated b
	u_b2 = grouped[pair[1]]								# users who rated b_id

	b1_users = list(u_b1.keys())
	b2_users = list(u_b2.keys())

	common_users = list(set(b1_users)&set(b2_users))
	u_b1 = {k: u_b1[k] for k in common_users}
	u_b2 = {k: u_b2[k] for k in common_users}

	pc = get_pearson_correlation(u_b1,u_b2)
	return (tuple((pair[0],pair[1],pc))) 

def get_corrated_users(tup):
	u_b1 = grouped[tup[0]]								# users who rated b
	u_b2 = grouped[tup[1]]								# users who rated b_id
	b1_users = list(u_b1.keys())
	b2_users = list(u_b2.keys())
	common_users = list(set(b1_users)&set(b2_users))
	return len(common_users)
			
def get_corrated_items(tup):
	u_i1 = grouped[tup[0]]								# users who rated b
	u_i2 = grouped[tup[1]]								# users who rated b_id
	b1_items = list(u_i1.keys())
	b2_items = list(u_i2.keys())
	common_items = list(set(b1_items)&set(b2_items))
	return len(common_items)

train_file = sys.argv[1] 
model_file = sys.argv[2]
cf_type = sys.argv[3]

t = time.time()

sc = SparkContext(appName="Filtering")
sc.setLogLevel("ERROR")

if cf_type == "item_based":

	ipRDD = sc.textFile(train_file). \
	 		map(lambda s:((json.loads(s)["business_id"]),([json.loads(s)["user_id"],json.loads(s)["stars"]])))

	businesses = ipRDD.map(lambda s: (s[1])).distinct().collect()
	grouped = ipRDD.groupByKey().map(lambda s:(s[0],list(s[1]))) \
				   .map(lambda s:(s[0],get_avg_rating(s[1]))).collectAsMap()
	pairs_list = []
	pairs_list.append(businesses)
	valid_pairs = sc.parallelize(pairs_list).flatMap(lambda s : combinations(s, 2)) \
					.map(lambda s:(s[0],s[1],get_corrated_users(s))) \
					.filter(lambda s: s[2] >= 3) \
					.map(lambda s: get_correlation((s[0],s[1]))) \
					.filter(lambda s: s[2] > 0).collect()

	with open(model_file,"w") as fp:
		for i in valid_pairs:
			json.dump({"b1":i[0],"b2":i[1],"sim":i[2]},fp)
			fp.write("\n")
	print("Done :- ",time.time()-t)

elif cf_type == "user_based":

	ipRDD = sc.textFile(train_file). \
	 		map(lambda s:((json.loads(s)["user_id"]),([json.loads(s)["business_id"],json.loads(s)["stars"]])))

	baskets = ipRDD.map(lambda s:(s[0],s[1][0]))
	distinct_business = baskets.map(lambda s:s[1]).distinct().collect()
	bucket_size = len(distinct_business)


	grouped = ipRDD.groupByKey().map(lambda s:(s[0],list(s[1]))) \
				   .map(lambda s:(s[0],get_avg_rating(s[1]))).collectAsMap()

	businesses = {k: v for v, k in enumerate(distinct_business)}

	baskets = baskets.mapValues(lambda s:businesses[s])
	sig_matrix = signature_matrix()		
	
	# Locality Sensitive Hashing
	n = 32
	B = 32
	R = 1
	length = len(sig_matrix.collect())
	# print("Length of signature matrix :- ", length)
	bandsRDD = sig_matrix.map(get_bands).flatMap(lambda s: s) \
					   .reduceByKey(lambda a,b: a+b) \
					   .filter(lambda s:len(s)>1) 

	pairs = bandsRDD.map(lambda s: (s[0], list(set(s[1])))) \
				.map(lambda s : list(combinations(s[1],2))) \
				.flatMap(lambda s: s).distinct()

	dataDict = baskets.groupByKey().mapValues(set).collectAsMap()
	valid_pairs = pairs.map(lambda s:(s[0],s[1],jaccard_similarity(s))) \
					   .filter(lambda s:(s[2] >= 0.01)) \
					   .map(lambda s:(s[0],s[1],get_corrated_items((s[0],s[1])))) \
					   .filter(lambda s:(s[2] >= 3)) \
					   .map(lambda s:(get_correlation((s[0],s[1]))))

	# print(valid_pairs.take(5))
	with open(model_file,"w") as fp:
		for i in valid_pairs.collect():
			json.dump({"u1":i[0],"u2":i[1],"sim":i[2]},fp)
			fp.write("\n")
else:
	print("Invalid argument")

print("Duration :- ",time.time()-t)