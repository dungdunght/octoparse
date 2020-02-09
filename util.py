import requests

def request_t_post(host, path, tokenStr, bodyData=''):
	return requests.post(host + path, headers={'Authorization': 'bearer ' + tokenStr}, data=bodyData).json()

def request_t_get(host, path, tokenStr):
	return requests.get(host + path, headers={'Authorization': 'bearer ' + tokenStr}).json()

def show_task_data(dataResult):
	if 'error' in dataResult:
		if dataResult['error'] == 'success' and 'dataList' in dataResult['data']:
			for dataDict in dataResult['data']['dataList']:
				for k, v in dataDict.items():
					pass
					# print("%s\t%s"%(k.encode('utf8'), v.encode('utf8')))
		else:
			print(dataResult['error_Description'].encode('utf8'))
	else:
		print(response)