from pyspark import SparkContext, SparkConf
import sys
import json
import os

conf = SparkConf().setAppName("inf553")
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")

input_file = sys.argv[1]
output_file = sys.argv[2]
partition_type = sys.argv[3]
n_partitions = int(sys.argv[4])
n = int(sys.argv[5])

def printf(iterator):
	yield len(list(iterator))

def star_partitioner(stars):
	return hash(stars)

def b_id_partitioner(business_id):
	return hash(business_id)

textRDD = sc.textFile(input_file)
# sampleRDD = sc.textFile(input_file)
# textRDD = sc.parallelize(sampleRDD.take(1000))

if partition_type == "default":
	output = {}
	output["n_partitions"] = textRDD.getNumPartitions()
	output["n_items"] = textRDD.mapPartitions(printf).collect()
	count = textRDD.map(lambda s: (json.loads(s)["business_id"],1)).countByKey()
	output["result"] = sc.parallelize(tuple(count.items())).filter(lambda s : s[1] > n).map(lambda s: [s[0],s[1]]).collect()
	with open(output_file,"w") as f:
		json.dump(output,f)

elif partition_type == "customized":
	output = {}
	output["n_partitions"] = n_partitions
	output["n_items"] = textRDD.map(lambda s:(json.loads(s)["business_id"],json.loads(s)["stars"])).partitionBy(n_partitions,star_partitioner).mapPartitions(printf).collect()
	output["result"] = sc.parallelize(tuple(textRDD.map(lambda s:(json.loads(s)["business_id"],json.loads(s)["stars"])).partitionBy(n_partitions,star_partitioner).countByKey().items())).filter(lambda s : s[1] > n).map(lambda s: [s[0],s[1]]).collect()

	with open(output_file,"w") as f:
		json.dump(output,f)

else:
	print("Invalid argument")
	sys.exit(1)