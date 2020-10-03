import sys
import pyspark
import time
from pyspark import SparkContext
import collections
import json
import string
import math
import operator
import re
from itertools import islice
import collections


def find_cosine_similarity(tup):
	if tup[0] not in user_profile.keys() or tup[1] not in business_profile.keys():
		return 0
	else:
		u_profile = user_profile[tup[0]]
		b_profile = business_profile[tup[1]]
		intersection = len(list(set(u_profile) & set(b_profile)))
		d1 = math.sqrt(len(u_profile))
		d2 = math.sqrt(len(b_profile))
		cosine_similarity = intersection/(d1 * d2)
		return cosine_similarity
		


def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))


test_file = sys.argv[1]
model_file = sys.argv[2]
output_file = sys.argv[3]

sc = SparkContext(appName="MinHash")
sc.setLogLevel("ERROR")


with open(model_file) as f:
	model = json.load(f)


business_profile = dict(model[0])
user_profile = dict(model[1])

testRDD = sc.textFile(test_file)
# print(testRDD.count())
prediction = testRDD.map(lambda s: (json.loads(s)["user_id"],json.loads(s)["business_id"])) \
					.map(lambda s: (s[0], s[1], find_cosine_similarity((s[0],s[1])))) \
					.filter(lambda s: s[2] >= 0.01).collect()
# print(prediction.count())


with open(output_file,"w") as fp:
	for s in prediction:
		json.dump({"user_id":s[0],"business_id":s[1],"sim":s[2]},fp)
		fp.write("\n")