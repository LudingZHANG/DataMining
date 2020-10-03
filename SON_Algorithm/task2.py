import sys
#import numpy as np
import time
from pyspark import SparkContext
import collections
from itertools import combinations
import math
from operator import add
import json
import csv

filter_threshold = int(sys.argv[1])
support = int(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]

sc = SparkContext(appName="SON")
sc.setLogLevel("OFF")

def printf(iterator):
	yield len(list(iterator))

def get_singleton_itemsets(par_baskets, threshold,frequent_items):
	item_count = {}
	singleton = [] 

	for i in par_baskets:
		#for l in i[1]:
		for l in i:
			if l not in item_count.keys():
				item_count[l] = 1
			else:
				item_count[l] += 1
			
	for key, val in item_count.items():
		if val >= threshold:
			singleton.append(key)
			frequent_items.append(key)

	return singleton,frequent_items

def get_candidate_itemsets(k,frequent_items):

	candidate_itemsets = []
	for i in range(len(frequent_items)-1):
		for j in range(i+1, len(frequent_items)):
			a = frequent_items[i]
			b = frequent_items[j]
			if a[0:(k-2)] == b[0:(k-2)]:
				candidate_itemsets.append(tuple(set(a) | set(b)))
			else:
				break
	candidate_itemsets = sorted(candidate_itemsets)			
	return candidate_itemsets

def get_frequent_items(par_baskets, candidate, sample_support):
    candidate_count = []
    for val in candidate:
        val = tuple(sorted(val))
        for item in par_baskets:
            if set(val).issubset(item):
                if val not in candidate_count:
                    candidate_count[val] = 1
                else:
                    candidate_count[val]+= 1
    frequent_pairs = [x for x in candidate_count if candidate_count[x] >= sample_support]
    return sorted(tuple(frequent_pairs))


def inpass(iterator):

	#print("In inpass")
	par_baskets = list(iterator)
	threshold = support*(len(par_baskets)/total_baskets)	
	frequent_items = []

	t = time.time()
	singleton,frequent_items = get_singleton_itemsets(par_baskets,threshold,frequent_items)
	singleton = sorted(singleton)
	# print("In k = 1 :- ",time.time()-t,len(singleton))
	
	t = time.time()
	candidate_itemsets = set(combinations(singleton,2))
	# print("Candidates :- ",time.time()-t,len(candidate_itemsets))

	freq = {}
	for c in candidate_itemsets:
		c = tuple(sorted(c))
		for basket in par_baskets:
			if set(c).issubset(basket):
				if c not in freq:
					freq[c] = 1
				else:
					freq[c] += 1

	freq_pairs = [x for x in freq if freq[x] >= threshold]
	frequent_k = sorted(freq_pairs)	
	#frequent_k = get_frequent_items(candidate_itemsets,par_baskets,threshold)
	frequent_items.extend(frequent_k)

	# print("In k = 2 :- ",time.time()-t)
	k = 3

	while True:
		
		t = time.time()
		# print("Here k is ",k, len(frequent_k))
		candidate_itemsets = get_candidate_itemsets(k,frequent_k)
		# print("Candidates :- ",time.time()-t,len(candidate_itemsets))

		freq = {}
		for c in candidate_itemsets:
			c = tuple(sorted(c))
			for basket in par_baskets:
				if set(c).issubset(basket):
					if c not in freq:
						freq[c] = 1
					else:
						freq[c] += 1

		freqitems = [x for x in freq if freq[x] >= threshold]
		frequent_k = sorted(tuple(freqitems))	
		frequent_items.extend(frequent_k)
		# print("In k = ",k," :- ",time.time()-t)

		if len(freqitems) == 0:
			break	
		else:
			k += 1


	yield frequent_items			

def get_count(iterator,candidates):
	par_baskets = list(iterator)
	output = [] 

	for cand in candidates:
		count = 0
		if type(cand) is tuple:
			for basket in par_baskets:
				if set(cand).issubset(basket):
					count += 1
		else:
			for basket in par_baskets:
				if cand in basket:
					count += 1

		output.append((cand,count))
	yield output	

def write_list(string,frequent,output_file,mode):
	if len(frequent) == 0:
		max_len = 0
	else:
		max_len = max([len(c) for c in frequent])

	tup_count = 2 
	others = []  
	singles = [] 
	for idx,f in enumerate(frequent): 
		if type(f) is not tuple: 
			singles.append(tuple([f])) 

	while tup_count < max_len + 1:
		l = []  
		for idx,f in enumerate(frequent): 
			if type(f) is tuple and len(f) == tup_count: 
				l.append(f)  
		others.append(l) 
		tup_count += 1 

	with open(output_file,mode) as f:
		f.write(string)
		f.write("\n")
		l = ','.join('({})'.format("'"+str(t[0])+"'") for t in sorted(singles))
		f.write(l)
		f.write("\n\n")

		for idx,i in enumerate(others):
			if len(i) != 0:
				l = "),".join(str(sorted(i))[1:-1].split("), "))
				f.write(l)
			# if idx == len(others)-1:
			# 	break
				f.write("\n\n")
		# if mode == "w":
		# 	f.write("\n\n")
		f.close()
	return


start = time.time()
ubRDD = sc.textFile(input_file)

header = ubRDD.first() 
baskets = ubRDD.filter(lambda row: row != header).map(lambda s: s.split(",")) \
				.map(lambda s:((s[0]),(s[1]))).groupByKey().mapValues(set) \
				.map(lambda s: s[1]) \
				.filter(lambda s:len(s) > filter_threshold).persist()


total_baskets = baskets.count()
numPartitions = baskets.getNumPartitions()
# print("Total number of baskets :- ",total_baskets)
# print("Total number of partitions :- ",numPartitions)
#print(baskets.mapPartitions(printf).collect())
#sys.exit(1)

# Phase 1
candidates = baskets.mapPartitions(inpass).flatMap(lambda s: [(i,1) for i in s]).keys().distinct().collect()
# print("Phase 1 completed")
write_list("Candidates:",candidates,output_file,"w")
# print("Candidates :- ",len(candidates))

# Phase 2
frequent_output = baskets.mapPartitions(lambda b: get_count(b,candidates)) \
				  .flatMap(lambda x: x).reduceByKey(lambda v1,v2: v1+v2) \
				  .filter(lambda s : s[1] >= support).keys().collect()

write_list("Frequent Itemsets:",frequent_output,output_file,"a")
# print("Frequent Itemsets :- ",len(frequent_output))
print("Duration : ",time.time() - start)		