import requests
headers={'Referer':'https://finance.sina.com.cn'}
url='https://hq.sinajs.cn/list=sh600000'
file = requests.get(url=url, headers=headers)
print(file.text)