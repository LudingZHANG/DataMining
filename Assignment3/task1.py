import sys
import pyspark
import time
from pyspark import SparkContext
import collections
from itertools import combinations
import math
import json
from itertools import islice


def jaccard_similarity(m):
	a = dataDict[m[0]]
	b = dataDict[m[1]]
	intersection = len(a & b)
	union = len(a) + len(b) - intersection
	return float(intersection)/float(union)


def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))

def append(list1,list2):
	list1.append(list2)
	return list1

def hashing(a,b):
	return baskets.map(lambda x : (x[0], ((a * x[1]+ b) % bucket_size))) \
			   .groupByKey().mapValues(list).map(lambda x : (x[0], min(x[1])))

def get_bands(tup):
	bands = []
	b_id = tup[0]
	signatures = tup[1]
	idx = 0
	for i in range(0,B):
		row = []
		for j in range(0,R):
			row.append(signatures[idx])
			idx+=1
		bands.append(((i,tuple(row)),[b_id]))
	return bands

def signature_matrix():

	# hash1 = hashing(7,5)
	# hash2 = hashing(11,13)
	# hash3 = hashing(17,41)
	# hash4 = hashing(23,53)
	# hash5 = hashing(41,31)
	# hash6 = hashing(23,7)
	# hash7 = hashing(83,161)
	# hash8 = hashing(79,83)
	# hash9 = hashing(93,97)
	# hash10 = hashing(87,7)
	# hash11 = hashing(17,19)
	# hash12 = hashing(13,23)
	# hash13 = hashing(11,17)
	# hash14 = hashing(13,61)
	# hash15 = hashing(79,19)
	# hash16 = hashing(23,41)

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
    
	# hash1 = hashing(8597,48611)
	# hash2 = hashing(3191,13099)
	# hash3 = hashing(2393,11909)
	# hash4 = hashing(2383,166331)
	# hash5 = hashing(4591,17033)
	# hash6 = hashing(2371,11909)
	# hash7 = hashing(7411,166331)
	# hash8 = hashing(2393,48611)
	# hash9 = hashing(3191,8597)
	# hash10 = hashing(87,11909)
	# hash11 = hashing(177,166331)
	# hash12 = hashing(2393,7879)
	# hash13 = hashing(4591,21269)
	# hash14 = hashing(3191,48611)
	# hash15 = hashing(7879,11909)
	# hash16 = hashing(2371,21269)
	# hash17 = hashing(4591,21269)
	# hash18 = hashing(7879,48611)
	# hash19 = hashing(3191,41179)
	# hash20 = hashing(143,31511)
	# hash21 = hashing(6079,41179)
	# hash22 = hashing(191,166331)
	# hash23 = hashing(6079,11909)
	# hash24 = hashing(4591,15349)
	# hash25 = hashing(919,21269)
	# hash26 = hashing(23,4591)
	# hash27 = hashing(871,6079)
	# hash28 = hashing(7879,31511)
	# hash29 = hashing(2393,1667)
	# hash30 = hashing(4591,11909)
	# hash31 = hashing(2393,166331)
	# hash32 = hashing(409,11909)
	# hash33 = hashing(3191,15349)
	# hash34 = hashing(4591,31511)
	# hash35 = hashing(8597,21269)
	# hash36 = hashing(4591,31511)
	# hash37 = hashing(2381,48611)
	# hash38 = hashing(3191,41179)
	# hash39 = hashing(2381,31511)
	# hash40 = hashing(6079,41179)
	# hash41 = hashing(2381,166331)
	# hash42 = hashing(6079,15349)
	# hash43 = hashing(2381,31511)
	# hash44 = hashing(919,15349)
	# hash45 = hashing(2381,48611)
	# hash46 = hashing(87,6079)
	# hash47 = hashing(6079,15349)
	# hash48 = hashing(2381,31511)
	# hash49 = hashing(78,15349)
	# hash50 = hashing(371,31511)


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
	# sig_matrix = sig_matrix.join(hash21).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash22).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash23).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash24).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash25).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash26).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash27).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash28).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash29).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash30).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash31).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash32).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash33).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash34).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash35).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash36).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash37).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash38).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash39).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash40).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash41).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash42).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash43).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash44).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash45).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash46).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash47).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash48).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash49).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash50).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# hash1 = hashing(7,11)
	# hash2 = hashing(2,13)
	# hash3 = hashing(51,58)
	# hash4 = hashing(14,38)
	# hash5 = hashing(45,98)
	# hash6 = hashing(23,29)
	# hash7 = hashing(3,2)
	# hash8 = hashing(13,19)
	# hash9 = hashing(11,17)
	# hash10 = hashing(87,109)
	# hash11 = hashing(177,83)
	# hash12 = hashing(23,73)
	# hash13 = hashing(43,87)
	# hash14 = hashing(5,7)
	# hash15 = hashing(89,22)
	# hash16 = hashing(46,42)
	# hash17 = hashing(41,51)
	# hash18 = hashing(67,43)
	# hash19 = hashing(111,82)
	# hash20 = hashing(143,41)
	# hash21 = hashing(11,14)
	# hash22 = hashing(19,23)
	# hash23 = hashing(29,67)
	# hash24 = hashing(89,111)
	# hash25 = hashing(7,19)
	# hash26 = hashing(17,23)
	# hash27 = hashing(93,87)
	# hash28 = hashing(11,72)
	# hash29 = hashing(81,9)
	# hash30 = hashing(78,24)
	# hash31 = hashing(36,12)
	# hash32 = hashing(42,24)

	# sig_matrix = hash1.join(hash2).mapValues(list)
	# sig_matrix = sig_matrix.join(hash3).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash4).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash5).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash6).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash7).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash8).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash9).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash10).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash11).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash12).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash13).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash14).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash15).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash16).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash17).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash18).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash19).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash20).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash21).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash22).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash23).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash24).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash25).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash26).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash27).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash28).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash29).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash30).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash31).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))
	# sig_matrix = sig_matrix.join(hash32).mapValues(list).map(lambda s:(s[0],append(s[1][0],s[1][1])))

	sig_matrix = sig_matrix.sortByKey()
	return sig_matrix


sc = SparkContext(appName="MinHash")
sc.setLogLevel("ERROR")

input_file = sys.argv[1]
output_file = sys.argv[2]

t = time.time()
baskets = sc.textFile(input_file).map(lambda s:(json.loads(s)["business_id"],json.loads(s)["user_id"]))
distinct_users = baskets.map(lambda s:s[1]).distinct().collect()
bucket_size = len(distinct_users)
user = {k: v for v, k in enumerate(distinct_users)}

# Min Hashing
baskets = baskets.mapValues(lambda s:user[s])
sig_matrix = signature_matrix()

candidatePairs = []
# Locality Sensitive Hashing
n = 10
B = 10
R = 1

pairs = sig_matrix.map(get_bands).flatMap(lambda s: s) \
					   .reduceByKey(lambda a,b: a+b) \
					   .filter(lambda s:len(s)>1) \
					   .map(lambda s: (s[0], list(set(s[1])))) \
					   .map(lambda s : list(combinations(s[1], 2))) \
					   .flatMap(lambda s: s).distinct()

dataDict = baskets.groupByKey().mapValues(set).collectAsMap()
valid_pairs = pairs.map(lambda s:(s[0],s[1],jaccard_similarity(s))).filter(lambda s:(s[2] >= 0.05))

with open(output_file,"w") as fp:
	for c in valid_pairs.collect():
		json.dump({"b1":str(c[0]),"b2":str(c[1]),"sim":c[2]},fp)
		fp.write("\n")
print("Duration :- ",time.time()-t)
