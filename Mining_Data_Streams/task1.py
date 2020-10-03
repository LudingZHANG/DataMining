import time
import os
import sys
from pyspark import SparkContext
import timeit
import math
from itertools import islice
import random
import copy
import json
import csv
import binascii

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
		primes = [i for i in range(10000,50000) if isPrime(i)]
		a = random.randint(10000,50000)
		b = random.randint(10000,50000)	
		pr = random.choice(primes)
		hash_list.append((a,b,pr))
	return hash_list	


def convert_str_to_int(string):
	if len(string) == 0:
		return -999999
	else:
		return int(binascii.hexlify(string.encode('utf8')),16)
	 

def add_element(el, m, hash_list):
	values = []
	for i in range(0,len(hash_list)):
		value = ((hash_list[i][0] * el + hash_list[i][1]) % hash_list[i][2]) % m
		values.append(value)
	return values


def check_element(el, m, hash_list, city1, k):
	values = []
	for i in range(0,len(hash_list)):
		value = ((hash_list[i][0] * el + hash_list[i][1]) % hash_list[i][2]) % m
		values.append(value)

	if len(set(city1)&set(values)) == k:
		return 1
	else: 
		return 0

sc = SparkContext()
sc.setLogLevel("OFF")

first_json_path = sys.argv[1]
second_json_path = sys.argv[2]
output_file_path = sys.argv[3]


city_business1 = sc.textFile(first_json_path) \
				   .map(lambda s:json.loads(s)["city"]) \
				   .map(lambda s:convert_str_to_int(s))


city_business2 = sc.textFile(second_json_path) \
				   .map(lambda s:json.loads(s)["city"]) \
				   .map(lambda s:convert_str_to_int(s))

num_cities = city_business1.count()
# p = 0.01
# p = 0.1

# m = -(num_cities * math.log(p))/(math.log(2)**2)
# m = int(m)


# k = int((m/num_cities) * math.log(2))
k = 6
m = (k * num_cities)/(math.log(2))
m = int(m)
# print(num_cities,m,k)

hash_list = generate_hash_functions(k)
# print(hash_list)

city_business1_build = city_business1.filter(lambda s: s != -999999) \
						.map(lambda s:add_element(s,m,hash_list)) \
					    .flatMap(lambda s :s).distinct()
# print(city_business1_build.take(5),city_business1_build.count())
city_business1_build = city_business1_build.collect()

city = city_business2.map(lambda s:check_element(s, m, hash_list, city_business1_build, k)).collect()
# print(city_business2.count(),len(city))

with open(output_file_path,"w") as fp:
	fp.write(str(city).replace(",","").replace("[","").replace("]","").replace('"',''))
	# writer.writerow(city)