#!/usr/bin/python
#coding=utf-8
import sys
import requests
import util
import os
import random
import getpass
import samples
import json
import pymongo
from datetime import datetime


def log_in(base_url, username, password): 	
	"""login and get a access token
	
	Arguments:
			base_url {string} -- authrization base url(currently same with api)
			username {[type]} -- your username
			password {[type]} -- your password
	
	Returns:
			json -- token entity include expiration and refresh token info like:
					{
							"access_token": "ABCD1234",      # Access permission
							"token_type": "bearer",		 # Token type
							"expires_in": 86399,		 # Access Token Expiration time (in seconds)(It is recommended to use the same token repeatedly within this time frame.) 
							"refresh_token": "refresh_token" # To refresh Access Token
					}
	"""
	print('Get token:')
	content = 'username={0}&password={1}&grant_type=password'.format(username, password)
	token_entity = requests.post(base_url + 'token', data = content).json()

	if 'access_token' in token_entity:
		print(token_entity)
		return token_entity
	else:
		print(token_entity['error_description'])
		os._exit(-2)

def get_data(base_url, token_entity, task_id, offset = 0, size = 1000):
	"""Start the samples test
	
	Arguments:
			base_url {string} -- api base url, like http://advancedapi.octoparse.com/ (for China: http://advancedapi.bazhuayu.com/)
			token_entity {json} -- token entity after logged in
	"""
	samples.refresh_token(base_url, token_entity['refresh_token'])

	token = token_entity['access_token']
	result = samples.get_data_by_offset(base_url, token, task_id, offset, size)
	return result

if __name__ == '__main__':
	if len(sys.argv) < 2:
		print('Please sepecify your mode!')
		os._exit(-1)
	
	mode = sys.argv[1]

	user_name = "dungdunght"
	password = "drogba"
	
	base_url = 'http://dataapi.octoparse.com/'
	token_entity = log_in(base_url, user_name, password)

	with open('/home/dungnd/octoparse_to_mongo/config.json') as f:
		config = json.load(f)
	if mode == 'add_data':
		for link in config:
			task_id = link["id"]
			source = link["source"]
			times = link["times"] if ('times' in link) else 1
			type = link["type"]
			offset = 0
			for time in range(times):
				dataResult = get_data(base_url, token_entity, task_id, offset)
				if 'error' in dataResult:
					if dataResult['error'] == 'success' and 'dataList' in dataResult['data']:
						list_result = dataResult['data']['dataList']
						myclient = pymongo.MongoClient("mongodb://admin:56879101112@localhost/admin")
						mydb = myclient["News"]
						for article in range(len(list_result)):
							time = list_result[article]["time"]
							link = list_result[article].get("link", "")
							title = list_result[article].get("title", "")
							try:
								time = datetime.strptime(time, '%d/%m/%Y')
								time = time.strftime('%d/%m/%Y')
							except:
								if ('time_by_hour' in link) and link["time_by_hour"]:
									time = datetime.today().strftime('%d/%m/%Y')
								else:
									time = "Undefined"
							list_result[article]["time"] = time
							if not len(link) or not len(title):
								time = "Undefined"
							current_collection = mydb[time]
							list_result[article]["source"] = source
							list_result[article]["type"] = type
							if time != "Undefined":
								try:
									current_collection.create_index([("source", pymongo.ASCENDING), ("title", pymongo.ASCENDING)], unique = True)
									current_collection.insert_one(list_result[article])
								except pymongo.errors.DuplicateKeyError:
									continue
							else:
								current_collection.update(list_result[article], list_result[article], upsert=True)
						offset = dataResult['data']['offset']

	elif mode == 'delete_data':
		for x in config:
			task_id = x["id"]
			url = 'api/task/RemoveDataByTaskId?taskId=' + task_id 
			util.request_t_post(base_url, url, token_entity['access_token'])

# End
		
