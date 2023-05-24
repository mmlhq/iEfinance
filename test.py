# -*- coding: UTF-8 -*-
# 开盘后每30s查询一次概念，对概念涨副超过3%，所属概念个股涨幅小于1%的进行记录（时间、股价、涨幅），
# 按分值排序，*如果有效，后期改钉钉推送，自动买卖

import re
import requests
import json
import random
import pymysql
import time
import baostock


millis = int(round(time.time() * 1000))
expando = "jQuery" + ("3.5.1" + '{:.16f}'.format(random.random())).replace(".", "")
cb = expando + "_" + str(millis)

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.42"}
url_concept = "http://14.push2.eastmoney.com/api/qt/clist/get?"
cb_concept = cb
millis_concept = str(millis)
params_concept = {
    'cb': cb_concept,
    'pn': '1',
    'pz': '20',
    'po': '1',
    'np': '1',
    'ut': 'bd1d9ddb04089700cf9c27f6f7426281',
    'fltt': '2',
    'invt': '2',
    'wbp2u': '|0|0|0|web',
    'fid': 'f3',
    'fs': 'm:90 t:3 f:!50',
    'fields': 'f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222',
    '_': millis_concept
}

response = requests.get(url=url_concept, headers=headers, params=params_concept)
msg = response.content.decode()
datas = re.findall('jQuery.+\((.+)\)', msg)
dict_datas = json.loads(datas[0])
concepts = dict_datas["data"]["diff"]

millis = int(round(time.time() * 1000))
expando = "jQuery" + ("1.12.3" + '{:.16f}'.format(random.random())).replace(".", "")
cb = expando + "_" + str(millis)
url_index = "http://push2.eastmoney.com/api/qt/clist/get?"
params_index = {
    'np': '1',
    'fltt': '1',
    'invt': '2',
    'cb': cb,
    'fs': 'b:BK1137',
    'fields': 'f14,f12,f13,f1,f2,f4,f3,f152,f128,f140,f141,f62,f184,f66,f69,f72,f75,f78,f81,f84,f87,f109,f160,f164,f165,f166,f167,f168,f169,f170,f171,f172,f173,f174,f175,f176,f177,f178,f179,f180,f181,f182,f183',
    'fid': 'f62',
    'pn': '1',
    'pz': '1000',
    'po': '1',
    'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
    'wbp2u': '|0|0|0|web',
    '_': str(millis)
}

for concept in concepts:
    gain_concept = concept['f3']
    turn_concept = concept['f8']
    BK_index = 'b:' + concept['f12']  # b:BK1137
    if gain_concept > 1.5:
        # 获取个股信息
        params_index['fs'] = BK_index
        response_index = requests.get(url=url_index, headers=headers, params=params_index)
        msg_index = response_index.content.decode()
        datas_index = re.findall('jQuery.+\((.+)\)', msg_index)
        dict_datas_index = json.loads(datas_index[0])
        indexs = dict_datas_index["data"]["diff"]
        for index in indexs:
            code = index['f12']
            gain_index = index['f3']/100
            if (gain_concept - gain_index) > 0.8:
                print(f"code:{code},concept:{concept['f12']},gain_concept:{gain_concept},gain_index:{gain_index}")

print("结束")
