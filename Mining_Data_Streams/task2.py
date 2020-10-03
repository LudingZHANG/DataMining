# Flajolet-Martin algorithm
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import json
import os
import sys
import binascii
import random
import csv

def isPrime(num):
	# Corner cases 
	if (num <= 1) : 
		return False
	if (num <= 3) : 
		return True

	# This is checked so that we can skip  
	# middle five numbers in below loop 
	if (num % 2 == 0 or num % 3 == 0) : 
		return False

	i = 5
	while(i * i <= num) : 
		if (num % i == 0 or num % (i + 2) == 0) : 
			return False
		i = i + 6
	return True	

def generate_hash_functions(k):
	hash_list = []
	for i in range(0,k):	
		primes = [i for i in range(100000,500000) if isPrime(i)]
		a = random.randint(100000,500000)
		b = random.randint(100000,500000)	
		hash_list.append((a,b))
	return hash_list	

def convert_str_to_int(string):
	if len(string) == 0:
		return -999999
	else:
		# return int(binascii.hexlify(string.encode('utf8')),16)
		return int(binascii.hexlify(string),16)

def count_trailing_zeros(bit): 
	bit = bit[::-1] 
	count = 0
	for i in range(len(bit)): 
		if (bit[i] == '0'): 
			count += 1
		# if '1' comes then break 
		else: 
			break
	return count

def get_estimate(list_of_cities):

	final_R_values = []
	for h in range(0,num_hash_functions):
		max_trailing_count = -9999
		for c in list_of_cities:
			c_int = convert_str_to_int(c)
			value = (hash_list[h][0] * c_int + hash_list[h][1]) % 100
			binary = bin(value).split("b")[1]
			zero_count = count_trailing_zeros(binary)	
			if zero_count > max_trailing_count:
				max_trailing_count = zero_count
		final_R_values.append(2**max_trailing_count)

	grouped = [final_R_values[i:i+5] for i in range(0, len(final_R_values), 5)]
	means = []
	for group in grouped:
		means.append(sum(group)/len(group))

	n = len(means) 
	means.sort() 
	  
	if n % 2 == 0: 
		median1 = means[n//2] 
		median2 = means[n//2 - 1] 
		median = (median1 + median2)/2
	else: 
		median = means[n//2]

	return median

def sample(time,rdd):
	# print("========= %s =========" % str(time))
	data = rdd.collect()
	num_samples = len(data)
	# print("===================== ")
	
	list_of_cities = []
	for d in data:
		d = json.loads(d)
		city = d["city"].encode("utf-8") 
		list_of_cities.append(city)
	
	gt = len(set(list_of_cities))
	median = get_estimate(list_of_cities)
	# print(str(time),gt,median)
	with open(output_file,"a") as fp:
		writer = csv.writer(fp)
		writer.writerow([time,gt,median])
	return


port_no = int(sys.argv[1])
output_file = sys.argv[2]
num_hash_functions = 45

sc = SparkContext(appName="task2")
sc.setLogLevel("OFF")
ssc = StreamingContext(sc , 5)


with open(output_file,"w") as fp:
	writer = csv.writer(fp)
	writer.writerow(["Time","Ground Truth","Estimation"])

hash_list = generate_hash_functions(num_hash_functions)
lines = ssc.socketTextStream("localhost", port_no).window(30,10).foreachRDD(sample)

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate