import os
from pyspark import SparkContext
from graphframes import *
import sys
import time
import json
from itertools import combinations
from pyspark.sql import SQLContext,SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import Row
import pyspark 
from itertools import islice
from collections import deque
import copy


def take(n, iterable):
	"Return first n items of the iterable as a list"
	return list(islice(iterable, n))


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

def get_bfs(graph, root):
	visited, queue = [root], [root]
	edges = []

	while queue:
		vertex = queue.pop(0)
		
		for adj in graph[vertex]:
			if adj not in visited:
				visited.append(adj)
				queue.append(adj)
			pair = tuple(sorted((vertex,adj)))
			if pair not in edges:
				edges.append(pair)

	return ((visited,edges))




def get_score_per_vertex(adjacency, root):

	visited, queue = list(), [root]
	parent_dict = {}
	level_dict = {}
	label_dict = {}
	
	parent_dict[root] = []
	level_dict[root] = 0
	label_dict[root] = 1

	while queue:
		vertex = queue.pop(0)
		visited.append(vertex)
		for adj in adjacency[vertex]:	
			# print("of vertex ",adj)
			
			if adj not in parent_dict:
				parent_dict[adj] = []
			if adj not in level_dict:
				level_dict[adj] = None
			if adj not in label_dict:
				label_dict[adj] = None

			if adj in visited and level_dict[vertex] - level_dict[adj] == 1:
				# print(adj, "Was here")
				parent_dict[vertex].append(adj)
				# label_dict[vertex] += label_dict[adj]
			if adj not in visited:

					if level_dict[adj] is None:
						level_dict[adj] = level_dict[vertex] + 1

					if	label_dict[adj] is None:
						# print(adj, "Is none")
						label_dict[adj] = label_dict[vertex]

					elif label_dict[adj] is not None and level_dict[adj] != level_dict[vertex]:
						# print(adj, "Is not none")	
						label_dict[adj] += label_dict[vertex]
			
					if adj not in queue:
						queue.append(adj)

	# for v in visited[1:]:
	# 	label_dict[v] = sum([label_dict[l] for l in parent_dict[v]])

	# non_leaves = []
	# for val in parent_dict.values():
	# 	for v in val:
	# 		if v not in non_leaves:
	# 			non_leaves.append(v)

	# # leaf nodes
	# leaves = set(visited) - set(non_leaves)

	# edges with parents and children	
	# edges = {}
	# for adj in visited:
	# 	edges[adj] = []
	# 	edges[adj].append([(adj,x) for x in list(set(adjacency[adj]) & set(parent_dict[adj]))]) # edges with parent
	# 	edges[adj].append([(x,adj) for x in list(set(adjacency[adj]) - set(parent_dict[adj])) if level_dict[x] != level_dict[adj]]) # edges with children

	# credits = {}
	# Assign credit to leaves
	# for key in leaves:
	# 	credits[key] = 1
	# 	parents = parent_dict[key]
	# 	total_labels = 0
	# 	for p in parents:
	# 		total_labels += label_dict[p]
	# 		#for v in edges[key][0]:
	# 		pair = (key,p)
	# 		credits[pair] = float(credits[key] * label_dict[p])/total_labels
	# 		#credits[v] = float(credits[key] * label_dict[v[1]])/total_labels

	# level = max(list(level_dict.values())) - 1
	# while level != -1:
	# 	for key in visited[::-1]:
	# 		if  key in non_leaves and level_dict[key] == level:
	# 			# from children
	# 			credits[key] = 1 + float(sum([credits[k] for k in edges[key][1]]))
	# 			# to parents
	# 			parents = parent_dict[key]
	# 			total_labels = 0
	# 			for p in parents:
	# 				total_labels += label_dict[p]
	# 				#for v in edges[key][0]:
	# 				pair = (key,p)
	# 				#credits[v] = float(credits[key] * label_dict[v[1]])/total_labels
	# 				credits[pair] = float(credits[key] * label_dict[p])/total_labels
	# 	level -= 1
	credits = {}
	tup_scores = {}

	for key in visited[::-1]:
		credits[key] = 1

	for key in visited[::-1]:
		if key != root:
			tot_labels = 0
			parents = parent_dict[key]
			for p in parents:
				tot_labels += label_dict[p]
	
			for p in parents:
				p1,p2 = key,p
				if p1 < p2:
					pair = tuple((p1, p2))
				else:
					pair = tuple((p2, p1))

				if pair not in tup_scores:
					tup_scores[pair] = float(credits[p1] * label_dict[p2])/tot_labels
				else:
					tup_scores[pair] += float(credits[p1] * label_dict[p2])/tot_labels
				credits[p2] += float(credits[p1] * label_dict[p2])/tot_labels


	all_tup_scores = []	
	for key,val in tup_scores.items():
			all_tup_scores.append([key,val])

	return all_tup_scores


def is_graph_empty(graph):
	if len(graph) == 0:
		return True
	else:
		return False

def girvan_newman(graph):
	disconnected = []
	graph_new = copy.deepcopy(graph)

	while is_graph_empty(graph_new) != True:

		nodes = list(graph_new.keys())
		bfs = get_bfs(graph, nodes[0])
		disconnected.append(bfs)

		for v in bfs[0]:
			del graph_new[v]
	
	return disconnected

def get_modularity(disconnected,graph,m):
	modularity = 0
	for d in disconnected:
		nodes = d[0]
		Aij = 0
		for n1 in nodes:
			for n2 in nodes:
				Aij = 0
				adjlist = graph[n1]
				if n2 in adjlist:
					Aij = 1
				ki = len(graph[n1])
				kj = len(graph[n2])
				modularity += Aij - ((ki * kj)/(2*m))
	modularity = float(modularity)/(2*m)
	return modularity

def get_new_graph(disconnected):
	adjacency_stored = {}
	for key in disconnected:
		nodes = item[0]
		edges = item[1]
		for e in edges:
			if e[0] not in adjacency_stored:
				adjacency_stored[e[0]] = [e[1]]
			else:
				adjacency_stored[e[0]] += [e[1]]
			if e[1] not in adjacency_stored:
				adjacency_stored[e[1]] = [e[0]]
			else:
				adjacency_stored[e[1]] += [e[0]]
	return adjacency_stored

start= time.time()
scConf = pyspark.SparkConf() \
    .setAppName('task2')

sc = pyspark.SparkContext(conf = scConf)
sqlContext = SQLContext(sc)
sc.setLogLevel("OFF")


filter_threshold = int(sys.argv[1])
input_file = sys.argv[2]
betweenness_output_file = sys.argv[3]
community_output_file = sys.argv[4]

ipRDD = sc.textFile(input_file).map(lambda s:s.split(","))
header = ipRDD.first()
ipRDD = ipRDD.filter(lambda row : row != header)

user_business_map = ipRDD.map(lambda s:(s[0],s[1])).groupByKey().mapValues(list).collectAsMap()
unique_users = ipRDD.map(lambda s:s[0]).distinct().collect()

pairs_list = []
pairs_list.append(unique_users)
pairs = sc.parallelize(pairs_list).flatMap(lambda s : combinations(s, 2)) \
		  .map(lambda s:(s[0],s[1],get_common(s))) \
		  .filter(lambda s: (s[2] >= filter_threshold)) \
		  .map(lambda s:((s[0],s[1]),(s[1],s[0]))).flatMap(lambda s:s)

# MAIN GRAPH
distinct_users = pairs.flatMap(lambda s:s).distinct()
adjacency = pairs.groupByKey().mapValues(list).collectAsMap()
pairs_scores =  distinct_users.flatMap(lambda s:get_score_per_vertex(adjacency,s)) \
					  .reduceByKey(lambda a,b : a+b).map(lambda s:(s[0],float(s[1]/2))) \
					  .sortBy(lambda s:(-s[1],s[0][0])).collect()


with open(betweenness_output_file,"w") as fp:
	for p in pairs_scores:
		string_p = "("+str(p[0][0])+","+str(p[0][1])+"), "+str(p[1])
		fp.write(string_p)
		fp.write("\n")


max_edge = pairs_scores[0][0]
m = len(pairs_scores)
max_modularity = -1
communities = []
num_iterations = 0
adjacency_stored = copy.deepcopy(adjacency)
#while num_iterations <= 30:
while len(pairs_scores) > 0:
	try:
		if max_edge[0] in adjacency_stored:
			if max_edge[1] in adjacency_stored[max_edge[0]]:
				adjacency_stored[max_edge[0]].remove(max_edge[1])
		if max_edge[1] in adjacency_stored:
			if max_edge[0] in adjacency_stored[max_edge[1]]:
				adjacency_stored[max_edge[1]].remove(max_edge[0])
			
		disconnected = girvan_newman(adjacency_stored)
		mod = get_modularity(disconnected,adjacency,m)
		adjacency_stored = {}
		for key in disconnected:
			nodes = key[0]
			edges = key[1]
			for e in edges:
				if e[0] in adjacency_stored:
					adjacency_stored[e[0]] += [e[1]]
				else:
					adjacency_stored[e[0]] = [e[1]]
				if e[1] in adjacency_stored:
					adjacency_stored[e[1]] += [e[0]]
				else:
					adjacency_stored[e[1]] = [e[0]]
		new_nodes = sc.parallelize(list(adjacency_stored.keys()))
		pairs_scores = new_nodes.flatMap(lambda s : get_score_per_vertex(adjacency_stored,s)) \
						  .reduceByKey(lambda a,b : a+b).map(lambda s:(s[0],float(s[1]/2))) \
						  .sortBy(lambda s:(-s[1],s[0][0])).collect()
		max_edge = pairs_scores[0][0]
		if mod >= max_modularity:
			max_modularity = mod
			communities = disconnected

	except Exception as e:
		pass		
	num_iterations += 1

output = []
for c in communities:
	output.append((sorted(c[0]), len(c[0])))
output.sort(key = lambda s: s[0])
output.sort(key = lambda s: s[1])

with open(community_output_file,"w") as fp:
	for c in output:
		fp.write(str(c[0]).replace("[","").replace("]",""))
		fp.write("\n")
