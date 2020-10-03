import os
from pyspark import SparkContext
from graphframes import *
import sys
import time
import timeit
import json
from itertools import combinations
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import Row
import pyspark 

def get_common(pair):
	p0 = user_business_map[pair[0]]
	p1 = user_business_map[pair[1]]
	common_business = len(list(set(p0)&set(p1)))
	return common_business

def f(x):
    d = {}
    for i in range(len(x)):
        d[str(i)] = x[i]
    return d

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages graphframes:graphframes:0.6.0-spark2.3-s_2.11 pyspark-shell'

# sc = SparkContext(appName="MinHash")
# sqlContext = SQLContext(sc)

start= timeit.default_timer()

scConf = pyspark.SparkConf() \
    .setAppName('task1') \
    .setMaster('local[3]')

sc = pyspark.SparkContext(conf = scConf)
ss = SparkSession(sc)
sqlContext = SQLContext(sc)
sc.setLogLevel("OFF")


filter_threshold = int(sys.argv[1])
input_file = sys.argv[2]
community_output_file = sys.argv[3]

ipRDD = sc.textFile(input_file).map(lambda s:s.split(","))
header = ipRDD.first()
ipRDD = ipRDD.filter(lambda row : row != header)

user_business_map = ipRDD.map(lambda s:(s[0],s[1])).groupByKey().mapValues(list).collectAsMap()
unique_users = ipRDD.map(lambda s:s[0]).distinct().collect()

pairs_list = []
pairs_list.append(unique_users)

# pairs = sc.parallelize(pairs_list).flatMap(lambda s : combinations(s, 2)) \
# 		  .map(lambda s:(s[0],s[1],get_common(s))) \
# 		  .filter(lambda s: (s[2] >= filter_threshold)) \
# 		  .map(lambda s:(s[0],s[1]))

pairs = sc.parallelize(pairs_list).flatMap(lambda s : combinations(s, 2)) \
		  .map(lambda s:(s[0],s[1],get_common(s))) \
		  .filter(lambda s: (s[2] >= filter_threshold)) \
		  .map(lambda s:((s[0],s[1]),(s[1],s[0]))).flatMap(lambda s:s)


distinct_users = pairs.flatMap(lambda s: s).distinct().map(lambda s: [s])
vertices = sqlContext.createDataFrame(distinct_users).toDF('id')
edges = pairs.map(lambda x: Row(**f(x))).toDF(['src','dst'])

graph = GraphFrame(vertices, edges)
communities = graph.labelPropagation(maxIter = 5).rdd.map(list) \
												 .map(lambda s:(s[1],s[0])) \
												 .groupByKey().map(lambda s:(s[0],sorted(list(s[1])))) \
												 .map(lambda s:(((s[0],s[1]),len(s[1])))) \
												 .map(lambda s:(s[0][1],s[1])) \
												 .sortBy(lambda s:s[0]) \
												 .sortBy(lambda s:s[1]) \
												 #.map(lambda s:s[0][1])
# print(communities.take(20))
# sys.exit(1)
with open(community_output_file,"w") as fp:
	for c in communities.collect():
		fp.write(str(c[0]).replace("[","").replace("]",""))
		fp.write("\n")

stop = timeit.default_timer()
print("Duration :- ",stop-start)