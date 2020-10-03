import sys
import pyspark
import time
from pyspark import SparkContext,StorageLevel,SparkConf
import collections
from itertools import combinations
import math
from operator import add


#conf = pyspark.SparkConf().setAll([('spark.executor.memory', '4g'), ('spark.executor.cores', '3'), ('spark.cores.max', '3'), ('spark.driver.memory','4g')])
sc = SparkContext(appName="SON")
sc.setLogLevel("ERROR")

case_number = int(sys.argv[1])
support = int(sys.argv[2])
input_file = sys.argv[3]
output_file = sys.argv[4]

def printf(iterator):
	yield len(list(iterator))

def get_singleton_itemsets(par_baskets, threshold,frequent_items):
	item_count = {}
	singleton = [] 

	for i in par_baskets:
		for l in i[1]:
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

def inpass(iterator):

	#print("In inpass")
	par_baskets = list(iterator)
	threshold = support*(len(par_baskets)/total_baskets)	
	frequent_items = []

	t = time.time()
	singleton,frequent_items = get_singleton_itemsets(par_baskets,threshold,frequent_items)
	singleton = sorted(singleton)
	
	freq = {}
	candidate_itemsets = list(combinations(singleton,2))

	for c in candidate_itemsets:
		c = tuple(sorted(c))
		for basket in par_baskets:
			if set(c).issubset(basket[1]):
				if c not in freq.keys():
					freq[c] = 1
				else:
					freq[c] += 1

	freq_pairs = {x:freq[x] for x in freq if freq[x] >= threshold}
	frequent_k = sorted(freq_pairs)	
	frequent_items.extend(frequent_k)

	k = 3
	while True:
		
		t = time.time()
		# print("Here k is ",k, len(frequent_k))
		candidate_itemsets = get_candidate_itemsets(k,frequent_k)
		#print("Candidates :- ",time.time()-t)

		freq = {}
		for c in candidate_itemsets:
			c = tuple(sorted(c))
			for basket in par_baskets:
				if set(c).issubset(basket[1]):
					if c not in freq:
						freq[c] = 1
					else:
						freq[c] += 1

		freqitems = {x for x in freq if freq[x] >= threshold}
		frequent_k = sorted(freqitems)	
		frequent_items.extend(frequent_k)
		#print("In k = ",k," :- ",time.time()-t)

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
				if set(cand).issubset(basket[1]):
					count += 1
		else:
			for basket in par_baskets:
				if cand in basket[1]:
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
			l = "),".join(str(sorted(i))[1:-1].split("), "))
			f.write(l)
			if idx == len(others)-1:
				break
			f.write("\n\n")
		if mode == "w":
			f.write("\n\n")
		f.close()
	return

start = time.time()
ip = sc.textFile(input_file)
# extract header
header = ip.first() 
bRDD = ip.filter(lambda row: row != header).map(lambda s: s.split(",")).persist()

# frequent businesses
if case_number == 1:
	baskets = bRDD.map(lambda s:(s[0],s[1])).groupByKey().map(lambda s: (s[0],list(set(s[1])))).persist()

	total_baskets = baskets.count()
	numPartitions = baskets.getNumPartitions()
	# print("Total number of baskets :- ",total_baskets)
	# print("Total partitions :- ",numPartitions)
	# print(baskets.mapPartitions(printf).collect())

	# Phase 1
	t = time.time()
	candidates = baskets.mapPartitions(inpass).flatMap(lambda s: [(i,1) for i in s]).keys().distinct().collect()
	# print("Candidates extracted in ",time.time()-t)
	write_list("Candidates:",candidates,output_file,"w")
		
	# Phase 2
	t = time.time()
	frequent_output = baskets.mapPartitions(lambda b: get_count(b,candidates)).flatMap(lambda x: x).reduceByKey(lambda v1,v2: v1+v2).filter(lambda s : s[1] >= support).keys().collect()
	write_list("Frequent Itemsets:",frequent_output,output_file,"a")
	# print("Frequents in ",time.time()-t)
	
# frequent users
elif case_number == 2:
	baskets = bRDD.map(lambda s:(s[1],s[0])).groupByKey().map(lambda x: (x[0],list(set(x[1])))).persist()
	total_baskets = baskets.count()
	numPartitions = baskets.getNumPartitions()
	# print("Total number of baskets :- ",total_baskets)
	# print("Total partitions :- ",numPartitions)

	# Phase 1
	candidates = baskets.mapPartitions(inpass).flatMap(lambda s: [(i,1) for i in s]).keys().distinct().collect()
	# print("Candidates :- ")
	write_list("Candidates:",candidates,output_file,"w")
	
	# Phase 2
	frequent_output = baskets.mapPartitions(lambda b: get_count(b,candidates)).flatMap(lambda x: x).reduceByKey(lambda v1,v2: v1+v2).filter(lambda s : s[1] >= support).keys().collect()
	write_list("Frequent Itemsets:",frequent_output,output_file,"a")
	# print("Frequent :-")
else:
	print("Invalid argument")
	sys.exit(1)

print("Duration : ",time.time() - start)