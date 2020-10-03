import sys
import pyspark
import time
from pyspark import SparkContext, SparkConf
import collections
import json
import string
import math
import operator
import re
from itertools import islice
import collections


def take(n, iterable):
    "Return first n items of the iterable as a list"
    return list(islice(iterable, n))

def clean_reviews(text):
	punctuations = list(string.punctuation)
	numbers = string.printable[0:10]
	punctuations.extend(numbers)	
	text = text.lower()
	for s in punctuations:
		text = text.replace(s,"")
		text = text.replace("\n"," ")	
	# text = re.sub(r'[^A-Za-z]',' ',text).strip()
	resultwords = []
	for word in text.split():
		if len(word) > 1 and word not in stopwords:
			resultwords.append(word)
	text = " ".join(resultwords)
	# new_list.append(text) 
	return text

def intmapper(words_list):
    intmapper = []
    for i in words_list:
        intmapper.append(indexed_words[i])
    return intmapper

def remove_rare_words(text_list):
	filtered = [i for i in text_list if i not in infrequent_words]
	return filtered

def get_word_importance(text_list):
	word_count = collections.Counter(text_list)
	most_occurred = word_count.most_common(1)

	for key,val in word_count.items():
		word_count[key] = word_count[key]/(most_occurred[0][1])
		word_count[key] = word_count[key] * inv_doc_freq[key]

	important_words = list(dict(sorted(word_count.items(),key=operator.itemgetter(1),reverse=True)).keys())[0:200]
	return important_words

def get_idf(word_count):
	return math.log2(total_num_documents/word_count)


def getup(bizlist):
    up_vector = []
    for x in bizlist:
        up_vector.extend(final_business_profile[x])
    return list(set(up_vector))

sc = SparkContext(appName = "MinHash")
sc.setLogLevel("ERROR")

start = time.time()
input_file = sys.argv[1]
model_file = sys.argv[2]

# stopwords_file = sys.argv[3]
# stopwords = []
# with open(stopwords_file) as fp:
# 	for line in fp:
# 		stopwords.append(line.replace("\n",""))

stopwords = ["i","me","my","myself","we","our","ours","ourselves","you","your","yours","yourself","yourselves",\
"he","him","his","himself","she","her","hers","herself","it","its","itself","they","them","their","theirs",\
"themselves","what","which","who","whom","this","that","these","those","am","is","are","was","were","be",\
"been","being","have","has","had","having","do","does","did","doing","a","an","the","and","but","if","or",\
"because","as","until","while","of","at","by","for","with","about","against","between","into","through",\
"during","before","after","above","below","to","from","up","down","in","out","on","off","over","under",\
"again","further","then","once","here","there","when","where","why","how","all","any","both","each","few",\
"more","most","other","some","such","no","nor","not","only","own","same","so","than","too","very","can",\
"will","just","dont","should","now","would","might","us"]


ipRDD = sc.textFile(input_file)

business_profile = ipRDD.map(lambda x: (json.loads(x)['business_id'],json.loads(x)['text'])) \
		                .reduceByKey(lambda a,b:a+b).mapValues(clean_reviews)
total_num_documents = business_profile.count()
# print("Total number of documents :- ",total_num_documents)

distinct_words = business_profile.map(lambda s:s[1]).flatMap(lambda s: s.split(' ')).distinct().collect()
indexed_words = {k: v for v, k in enumerate(distinct_words)}
#print("Number of distinct words :- ",len(distinct_words))
business_profile = business_profile.mapValues(lambda x:x.split(' ')).mapValues(intmapper)
#print("Business profile :- ",business_profile.take(2))

summed = business_profile.map(lambda s:len(s[1])).sum()
# print("Total words :- ",summed)
rare_limit = 0.000001 * summed
words_with_count = business_profile.flatMap(lambda s:s[1]).map(lambda s:(s,1)).reduceByKey(lambda a,b:a+b)
# print("Len of words with count :- ",words_with_count.count())
# print(words_with_count.take(2))

infrequent_words = words_with_count.filter(lambda s: s[1] < rare_limit).collectAsMap()
# print("Number of infrequent_words :- ",len(infrequent_words))
business_profile_reduced = business_profile.mapValues(remove_rare_words) 
# print("Reduced word count :- ",business_profile_reduced.map(lambda s: len(s[1])).sum())

inv_doc_freq = business_profile_reduced.map(lambda s: (s[0],set(s[1]))).flatMap(lambda s: s[1]) \
			.map(lambda s: (s,1)).reduceByKey(lambda a,b: a+b) \
			.mapValues(get_idf).collectAsMap()

final_business_profile = business_profile_reduced.mapValues(get_word_importance).collectAsMap()

user_profile = ipRDD.map(lambda s: (json.loads(s)['user_id'],json.loads(s)['business_id'])) \
		  .groupByKey().mapValues(list).mapValues(getup).collectAsMap()

final = [final_business_profile,user_profile]

with open(model_file,'w',newline='')as f:
		#for r in final:
		json.dump(final,f)
		#f.write('\n')

