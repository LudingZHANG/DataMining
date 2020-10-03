# Reservoir Sampling
from pyspark import SparkContext
import json
import os
import sys
import binascii
import random
import csv
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream
import sys
from tweepy.streaming import StreamListener
import random



tweets_list  = []
hashtags_dict = {}
sequence_no = 0

class Listener(StreamListener):

	def on_status(self, status):
		global sequence_no
		global tweets_list
		global hashtags_dict

		tweet = status.text
		tags = status.entities["hashtags"]
		
		if len(tags) > 0:
			
			if sequence_no < 100:
				tweets_list.append(status)
				for t in tags:
					hashtag = t["text"]
					if hashtag not in hashtags_dict:
						hashtags_dict[hashtag] = 1
					else:
						hashtags_dict[hashtag] += 1
			else:
				random_prob = random.randint(0,sequence_no)

				if random_prob <= 100:
					r = random.randint(0, 99)
					# print(r)
					del_tweet = tweets_list[r]
					del_tags = del_tweet.entities["hashtags"]
					for t in del_tags:
						hashtag = t["text"]
						hashtags_dict[hashtag] -= 1
						if hashtags_dict[hashtag] == 0:
							del hashtags_dict[hashtag]

					tweets_list[r] = status
					tags = status.entities["hashtags"]
					for t in tags:
						hashtag = t["text"]
						if hashtag not in hashtags_dict:
							hashtags_dict[hashtag] = 1
						else:
							hashtags_dict[hashtag] += 1
				else:
					pass			

			hashtags_dict = dict(sorted(hashtags_dict.items(), key = lambda s : s[0]))
			hashtags_dict = dict(sorted(hashtags_dict.items(), key = lambda s : s[1], reverse = True))
			top_values = sorted(list(set(hashtags_dict.values())),reverse=True)[0:3]
			
			# print("The number of tweets with tags from the beginning: "+str(sequence_no+1))
			with open(output_file,"a") as fp:
				string = "The number of tweets with tags from the beginning: "+str(sequence_no+1)+"\n"
				fp.write(string)
				for key, val in hashtags_dict.items():
					if val in top_values:
						string = str(key)+" : "+str(val)
						fp.write(string+"\n")
						# print(key," : ",val)
				fp.write("\n")
						
			# print("\n")
			sequence_no += 1

		else:
			pass

	def on_error(self, status_code):
		print(status_code)
		return False

port_no = int(sys.argv[1])
output_file = sys.argv[2]

if os.path.exists(output_file):
	os.remove(output_file)

ACCESS_TOKEN = "<ACCESS_TOKEN>"
ACCESS_TOKEN_SECRET = "<ACCESS_TOKEN_SECRET>"
CONSUMER_KEY = "<CONSUMER_KEY>"
CONSUMER_SECRET = "<CONSUMER_SECRET>"

auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
api = API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
listener = Listener()
stream = Stream(auth=api.auth, listener=listener)

try:
	print('Start streaming.')
	stream.sample(languages=['en'])

except KeyboardInterrupt as e :
	print("Stopped.")

finally:
	print('Done.')
	stream.disconnect()
