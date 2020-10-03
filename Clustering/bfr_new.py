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

def take(n, iterable):
	"Return first n items of the iterable as a list"
	return list(islice(iterable, n))

def euclidean_distance(point1,point2):
	return math.sqrt(sum([(a - b) ** 2 for a, b in zip(point1,point2)]))	

def get_average(points_list):
	centroid = []
	num_points = len(points_list)
	for j in range(1,num_dimensions+1):
		summed = 0
		avg = 0
		for i in points_list:
			summed += i[j]
		avg = summed/num_points
		centroid.append(avg)
	return centroid


def KMeans(datapoints, num_clusters, num_dimensions):
	tolerance = 0.01
	max_iteration = 100

	intial_centroids = list(datapoints.values())[0:num_clusters]

	centroids = {}
	for idx,v in enumerate(intial_centroids):
		centroids[idx] = v

	for m in range(max_iteration):
		classifications = {}

		for i in range(0,num_clusters):
			classifications[i] = [] 	

		for featureset in datapoints:
			distances = [euclidean_distance(datapoints[featureset],centroids[centroid]) for centroid in centroids]
			classification = distances.index(min(distances))
			point = [featureset]
			point.extend(datapoints[featureset])
			classifications[classification].append(point)

		previous_centroids = dict(centroids)

		for classification in classifications:
			cluster_size = len(classifications[classification])
			if cluster_size != 0:
				centroids[classification] = get_average(classifications[classification])
			else:
				centroids[classification] = previous_centroids[classification]

		# if (previous_centroids == centroids):
		# 	break

		optimized = True
		for c in centroids:
			original_centroid = previous_centroids[c]
			current_centroid = centroids[c]
			summed = 0
			for j in range(0,num_dimensions):
				summed += (current_centroid[j] - original_centroid[j])/(original_centroid[j] * 100.0)
			if summed > tolerance:
				optimized = False

		if optimized == True:
			break			

	return classifications


def generate_initial_statistics(points_list,num_dimensions):
	N = len(points_list)
	SUM = [0] * num_dimensions
	SUMSQ = [0] * num_dimensions
	centroid = [0] * num_dimensions
	variance = [0] * num_dimensions
	std_dev = [0] * num_dimensions

	if N == 0:
		return (N,SUM,SUMSQ,centroid,variance,std_dev)
	else:
		for j in range(0,num_dimensions):
			summed = 0
			summed_sq = 0
			for i in range(0,N):
				val = points_list[i][j+1]
				summed += val
				summed_sq += math.pow(val,2)
			SUM[j] = summed
			SUMSQ[j] = summed_sq 

		centroid = [i/N for i in SUM]
		for j in range(0,num_dimensions):
			variance[j] = (SUMSQ[j]/N) - math.pow(SUM[j]/N,2)
			std_dev[j] = math.sqrt(variance[j])	
		return (N,SUM,SUMSQ,centroid,variance,std_dev)

def mahalanobis_distance(centroid,point,std_dev):
	norm = [0] * num_dimensions
	summed = 0
	for j in range(0,num_dimensions):
		if std_dev[j] == 0:
			norm[j] = (point[j] - centroid[j])
		else:
			norm[j] = (point[j] - centroid[j])/std_dev[j]
		summed += math.pow(norm[j],2)
	distance = math.sqrt(summed)
	return	distance

def assign_nearest_cluster_DS(points_dict, num_dimensions,alpha):
	
	for featureset in points_dict:
		point = points_dict[featureset]
		min_md = math.inf
		cluster_assigned = ""

		for k in discarded_set_summary:
			centroid = discarded_set_summary[k][3]
			stddev = discarded_set_summary[k][5]

			maha_dist = mahalanobis_distance(centroid,point,stddev)
			if maha_dist < min_md:
				min_md = maha_dist
				cluster_assigned = k	

		if min_md < alpha * math.sqrt(num_dimensions):
				updated_variance = [0] * num_dimensions
				updated_sttdev = [0] * num_dimensions
				summary = discarded_set_summary[cluster_assigned]
				updated_N = summary[0] + 1
				updated_SUM =  [a + b for a, b in zip(summary[1], point)]
				updated_SUMSQ = [a + math.pow(b,2) for a,b in zip(summary[2],point)]
				updated_centroid = [i/updated_N for i in updated_SUM]
				for j in range(num_dimensions):
					updated_variance[j] = (updated_SUMSQ[j]/updated_N) - math.pow((updated_SUM[j]/updated_N),2)
					updated_sttdev[j] = math.sqrt(updated_variance[j])
				discarded_set_summary.update({cluster_assigned:(updated_N,updated_SUM,updated_SUMSQ,updated_centroid,updated_variance,updated_sttdev)})
				# row = [featureset]
				# row.extend(point)
				discard_set[featureset] = cluster_assigned
		else:
			row = [featureset]
			row.extend(point)
			not_assigned.append(row)

	return not_assigned


def assign_nearest_cluster(points_dict, num_dimensions, alpha):
	
	for featureset in points_dict:
		point = points_dict[featureset]
		min_md = math.inf
		cluster_assigned = ""

		for k in discarded_set_summary:
			centroid = discarded_set_summary[k][3]
			stddev = discarded_set_summary[k][5]

			maha_dist = mahalanobis_distance(centroid,point,stddev)
			if maha_dist < min_md:
				min_md = maha_dist
				cluster_assigned = k	

		if min_md < alpha * math.sqrt(num_dimensions):
				updated_variance = [0] * num_dimensions
				updated_sttdev = [0] * num_dimensions
				summary = discarded_set_summary[cluster_assigned]
				updated_N = summary[0] + 1
				updated_SUM =  [a + b for a, b in zip(summary[1], point)]
				updated_SUMSQ = [a + math.pow(b,2) for a,b in zip(summary[2],point)]
				updated_centroid = [i/updated_N for i in updated_SUM]
				for j in range(num_dimensions):
					updated_variance[j] = (updated_SUMSQ[j]/updated_N) - math.pow((updated_SUM[j]/updated_N),2)
					updated_sttdev[j] = math.sqrt(updated_variance[j])
				discarded_set_summary.update({cluster_assigned:(updated_N,updated_SUM,updated_SUMSQ,updated_centroid,updated_variance,updated_sttdev)})
				# row = [featureset]
				# row.extend(point)
				discard_set[featureset] = cluster_assigned
		else:
			row = [featureset]
			row.extend(point)
			not_assigned.append(row)

	for point in not_assigned:
		min_md = math.inf
		cluster_assigned = ""

		for k in compressed_set_summary:
			centroid = compressed_set_summary[k][3]
			stddev = compressed_set_summary[k][5]

			maha_dist = mahalanobis_distance(centroid,point[1:],stddev)
			if maha_dist < min_md:
				min_md = maha_dist
				cluster_assigned = k	

		if min_md < alpha * math.sqrt(num_dimensions):
				updated_variance = [0] * num_dimensions
				updated_sttdev = [0] * num_dimensions
				summary = compressed_set_summary[cluster_assigned]
				updated_N = summary[0] + 1
				updated_SUM =  [a + b for a, b in zip(summary[1], point[1:])]
				updated_SUMSQ = [a + math.pow(b,2) for a,b in zip(summary[2],point[1:])]
				updated_centroid = [i/updated_N for i in updated_SUM]
				for j in range(num_dimensions):
					updated_variance[j] = (updated_SUMSQ[j]/updated_N) - math.pow((updated_SUM[j]/updated_N),2)
					updated_sttdev[j] = math.sqrt(updated_variance[j])
				compressed_set_summary.update({cluster_assigned:(updated_N,updated_SUM,updated_SUMSQ,updated_centroid,updated_variance,updated_sttdev)})
				#compressed_set.append(point)
				compressed_set[point[0]] = cluster_assigned
		else:
			retained_set.append(point)

	return not_assigned


def compute_cluster_dist(current_k, current_cluster, summary,num_dimensions):
	current_centroid = get_average(current_cluster)
	min_md = math.inf
	cluster_assigned = ""
	for k in summary:
		centroid = summary[k][3]
		std_dev = summary[k][5]
		mdist = mahalanobis_distance(centroid,current_centroid,std_dev)

		if mdist < min_md:
			min_md = mdist	
			cluster_assigned = k

	return cluster_assigned,min_md



sc = SparkContext()
sc.setLogLevel("OFF")

cluster_no = 0
input_path = sys.argv[1]
num_clusters = int(sys.argv[2])
out_file1 = sys.argv[3]
out_file2 = sys.argv[4]

discard_set = {}
retained_set = []
compressed_set = {}
not_assigned = []

discarded_set_summary = {}
compressed_set_summary = {}
intermediate_results = []
all_point_idx = []

start = timeit.default_timer()
files = sorted(os.listdir(input_path))

iterate = 1
alpha = 2
first_file = input_path+files[0]

# STEP a
t = time.time()
points_dictionary = sc.textFile(first_file).map(lambda s:(s.split(","))) \
			   .map(lambda s:[float(t) for t in s]).map(lambda s:(s[0],s[1:])).collectAsMap()
all_point_idx.extend(list(points_dictionary.keys()))
num_dimensions = len(list(points_dictionary.values())[0])

# Run kmeans on whole file and remove clusters having one point
# intial = KMeans(points_dictionary,2 * num_clusters,num_dimensions)
# temp = []

# for i in intial:
# 	# print(i,len(intial[i]))
# 	if len(intial[i]) == 1:
# 		for j in intial[i]:
# 			retained_set.append(j)
# 	elif len(intial[i]) > 1:
# 		for j in intial[i]:
# 			temp.append(j)		


# temp_dict = {}
# for t in temp:
# 	temp_dict[t[0]] = t[1:]

# print(len(points_dictionary),len(temp_dict))
# # STEP break
# num_sample = int(0.3 * len(temp_dict))
# random.seed(32)
# sample = list(temp_dict)[0:num_sample]
# rem_sample = set(temp_dict.keys()) - set(sample)


# STEP b
num_sample = int(0.5 * len(points_dictionary))
random.seed(20)
sample = random.sample(list(points_dictionary), num_sample)
rem_sample = set(points_dictionary.keys()) - set(sample)



sample_20 = {}
for s in sample:
	sample_20[s] = points_dictionary[s]

sample_80 = {}
for s in rem_sample:
	sample_80[s] = points_dictionary[s]



# STEP c
intial_clusters = KMeans(sample_20,num_clusters,num_dimensions)

# STEP d
for i in intial_clusters:
	discarded_set_summary[i] = generate_initial_statistics(intial_clusters[i],num_dimensions)
	for k in intial_clusters[i]:
		discard_set[k[0]] = i


# STEP e
not_assigned = assign_nearest_cluster_DS(sample_80,num_dimensions,alpha)

not_assigned_dict = {}
for i in not_assigned:
	not_assigned_dict[i[0]] = i[1:]

rem_clusters = KMeans(not_assigned_dict,min(5*num_clusters,len(not_assigned_dict)),num_dimensions)
for i in rem_clusters:
	if len(rem_clusters[i]) > 1:
		for j in rem_clusters[i]:
			if j in retained_set:
				retained_set.remove(j)
			compressed_set[j[0]] = i
		compressed_set_summary[i] = generate_initial_statistics(rem_clusters[i],num_dimensions)
	elif len(rem_clusters[i]) == 1:
		for k in rem_clusters[i]:
			retained_set.append(k)
cluster_no = 5*num_clusters
print(iterate,len(discard_set),len(compressed_set),len(retained_set),len(discarded_set_summary),len(compressed_set_summary))
intermediate_results.append([iterate,len(discarded_set_summary),len(discard_set),len(compressed_set_summary),len(compressed_set),len(retained_set)])


# STEP f
iterate += 1
next_files = files[1:]
for f in next_files:
	
	points_dictionary = sc.textFile(input_path+f).map(lambda s:(s.split(","))) \
			   .map(lambda s:[float(t) for t in s]).map(lambda s:(s[0],s[1:])).collectAsMap()
	all_point_idx.extend(list(points_dictionary.keys()))
	num_dimensions = len(list(points_dictionary.values())[0])

	# Step g, h, i
	if len(compressed_set_summary) != 0:
		assign_nearest_cluster(points_dictionary,num_dimensions,alpha)
		print(iterate,len(discard_set),len(compressed_set),len(retained_set),len(discarded_set_summary),len(compressed_set_summary))
				
		retained_set_dict = {}
		for i in retained_set:
			key,val = i[0],i[1:]
			retained_set_dict[key] = val

		rem_clusters = KMeans(retained_set_dict,min(5*num_clusters,len(retained_set_dict)),num_dimensions)	
		for i in rem_clusters:
			for j in rem_clusters[i]:
				if j in retained_set:
					retained_set.remove(j)				
				compressed_set[j[0]] = i
			if len(rem_clusters[i]) > 1:
				cluster_assigned,dist = compute_cluster_dist(i,rem_clusters[i],compressed_set_summary,num_dimensions)
				# alpha = 2
				if dist < alpha * math.sqrt(num_dimensions):
					summary = compressed_set_summary[cluster_assigned]

					new_N = len(rem_clusters[i])
					updated_N = summary[0] 
					updated_SUM = summary[1]
					updated_SUMSQ = summary[2]
					updated_centroid = summary[3]
					updated_variance = summary[4]
					updated_std_dev = summary[5]
					for point in rem_clusters[i]:
						updated_N = summary[0] + 1
						updated_SUM =  [a + b for a, b in zip(summary[1], point[1:])]
						updated_SUMSQ = [a + math.pow(b,2) for a,b in zip(summary[2],point[1:])]
						updated_centroid = [i/updated_N for i in updated_SUM]
						for j in range(num_dimensions):
							updated_variance[j] = (updated_SUMSQ[j]/updated_N) - math.pow((updated_SUM[j]/updated_N),2)
							updated_std_dev[j] = math.sqrt(updated_variance[j])
							compressed_set_summary.update({cluster_assigned:(updated_N,updated_SUM,updated_SUMSQ,updated_centroid,updated_variance,updated_std_dev)})
				else:
					compressed_set_summary[cluster_no] = generate_initial_statistics(rem_clusters[i],num_dimensions)
					cluster_no += 1
			elif len(rem_clusters[i]) == 1:
				for j in rem_clusters[i]:
					retained_set.append(j)
		print(iterate,len(discard_set),len(compressed_set),len(retained_set),len(discarded_set_summary),len(compressed_set_summary))
		intermediate_results.append([iterate,len(discarded_set_summary),len(discard_set),len(compressed_set_summary),len(compressed_set),len(retained_set)])
	else:
		not_assigned = assign_nearest_cluster_DS(points_dictionary, num_dimensions,alpha)
		not_assigned_dict = {}
		for i in not_assigned:
			not_assigned_dict[i[0]] = i[1:]

		rem_clusters = KMeans(not_assigned_dict,min(5*num_clusters,len(not_assigned_dict)),num_dimensions)

		for i in rem_clusters:
			if len(rem_clusters[i]) > 1:
				for j in rem_clusters[i]:
					if j in retained_set:
						retained_set.remove(j)					
					compressed_set[j[0]] = i
				compressed_set_summary[cluster_no] = generate_initial_statistics(rem_clusters[i],num_dimensions)
				cluster_no += 1
			elif len(rem_clusters[i]) == 1:
				for k in rem_clusters[i]:
					retained_set.append(k)
		print(iterate,len(discard_set),len(compressed_set),len(retained_set),len(discarded_set_summary),len(compressed_set_summary))
		intermediate_results.append([iterate,len(discarded_set_summary),len(discard_set),len(compressed_set_summary),len(compressed_set),len(retained_set)])


	iterate += 1

reversed_compressed = {}
for k,v in compressed_set.items():
    if v not in reversed_compressed.keys():
        reversed_compressed[v] = [k]
    else:
        reversed_compressed[v].append(k)

# Merge CS with DS
for k in compressed_set_summary:
	cs_centroid = compressed_set_summary[k][3]
	cs_stddev = compressed_set_summary[k][5]
	min_md = math.inf
	cluster_assigned = ""
	for j in discarded_set_summary:
		ds_centroid = discarded_set_summary[j][3]
		ds_stddev = discarded_set_summary[j][5]
		dist = mahalanobis_distance(ds_centroid,cs_centroid,ds_stddev)
		
		if dist < min_md:
			min_md = dist
			cluster_assigned = j
	# alpha = 2		
	if min_md < alpha * math.sqrt(num_dimensions):
		ds_summary = discarded_set_summary[cluster_assigned]
		cs_summary = compressed_set_summary[k]

		updated_N = ds_summary[0] + cs_summary[0] 
		updated_SUM = [a + b for a, b in zip(ds_summary[1], cs_summary[1])] 
		updated_SUMSQ = [a + math.pow(b,2) for a,b in zip(ds_summary[2],cs_summary[2])]
		updated_centroid = [i/updated_N for i in updated_SUM]
		updated_variance = ds_summary[4]
		updated_stddev = ds_summary[5]

		for j in range(num_dimensions):
			updated_variance[j] = (updated_SUMSQ[j]/updated_N) - math.pow((updated_SUM[j]/updated_N),2)
			updated_stddev[j] = math.sqrt(updated_variance[j])
		discarded_set_summary.update({cluster_assigned:(updated_N,updated_SUM,updated_SUMSQ,updated_centroid,updated_variance,updated_stddev)})
		for point_idx in reversed_compressed[k]:
			discard_set[point_idx] = cluster_assigned
print(iterate,len(discard_set),len(compressed_set),len(retained_set),len(discarded_set_summary),len(compressed_set_summary))


for p in all_point_idx:
	if p not in discard_set:
		discard_set[p] = -1


with open(out_file2,"w") as fp:
	s = ["round_id","nof_cluster_discard","nof_point_discard","nof_cluster_compression","nof_point_compression","nof_point_retained"]
	writer = csv.writer(fp)
	writer.writerow(s)
	for i in intermediate_results:
		writer.writerow(i)

final = {}
for key,val in discard_set.items():
	key = int(key)
	final[key] = val

del discard_set
del compressed_set
del retained_set

final = dict(sorted(final.items(),key=lambda s:int(s[0])))
with open(out_file1,"w") as fp:
	json.dump(final,fp)

