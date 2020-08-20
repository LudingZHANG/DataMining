from pyspark import SparkContext, SparkConf
import sys
import json
import os
import string
conf = SparkConf().setAppName("inf553")
sc = SparkContext(conf=conf)
sc.setLogLevel("OFF")

input_file = sys.argv[1]
output_file = sys.argv[2]
stopword_file = sys.argv[3]
given_year = int(sys.argv[4])
top_m_users = int(sys.argv[5])
top_n_words = int(sys.argv[6])

avoid = ["(", "[", ",", ".", "!", "?", ":", ";", "]", ")"]
stopwords = []
with open(stopword_file,"r") as f:
	for x in f.readlines():
		stopwords.append(x.replace("\n",''))

def remove_stopwords(text):
	new_text = []
	for i in text.lower().split():
		if i not in stopwords:
			for j in avoid:
				i = i.replace(j,'')
			new_text.append(i)
	return new_text

textRDD = sc.textFile(input_file)
#sampleRDD = sc.textFile(input_file)
#textRDD = sc.parallelize(sampleRDD.take(1000))

total_reviews = textRDD.count()

num_reviews = len(textRDD.filter(lambda s: int(json.loads(s)["date"].split(" ")[0].split("-")[0]) == given_year).collect())

num_distinct_users = len(textRDD.map(lambda s: json.loads(s)["user_id"]).distinct().collect())

users = textRDD.map(lambda s: (json.loads(s)["user_id"],1)).countByKey()
top_users = sc.parallelize(tuple(users.items())).sortBy(lambda x: (-x[1],x[0])).map(lambda s :[s[0],s[1]]).take(top_m_users)

word_count = textRDD.flatMap(lambda s: remove_stopwords(json.loads(s)["text"])).map(lambda x: (x,1)).countByKey()
frequent_words = sc.parallelize(tuple(word_count.items())).sortBy(lambda x: (-x[1],x[0])).keys().take(top_n_words)


output_dict = {"A":total_reviews,"B":num_reviews,"C":num_distinct_users,"D": top_users,"E":frequent_words}

with open(output_file,"w") as f:
	json.dump(output_dict, f)

