# 爬取华尔街见闻网离岸人民币汇率
#!/bin/python3
import time

import requests
import datetime
import json
from apscheduler.schedulers.blocking import BlockingScheduler


with open("config/config.json", encoding="utf-8") as f:
    cfg = json.load(f)
info = cfg["dingtalk"]

url2 = info["url"]
headers2 = {'Content-Type': 'application/json;charset=utf-8'}


url1 = "https://api-ddc-wscn.awtmt.com/market/real?fields=symbol%2Cen_name%2Cprod_name%2Clast_px%2Cpx_change%2Cpx_change_rate%2Chigh_px%2Clow_px%2Copen_px%2Cpreclose_px%2Cmarket_value%2Cturnover_volume%2Cturnover_ratio%2Cturnover_value%2Cdyn_pb_rate%2Camplitude%2Cdyn_pe%2Ctrade_status%2Ccirculation_value%2Cupdate_time%2Cprice_precision%2Cweek_52_high%2Cweek_52_low%2Cstatic_pe%2Csource&prod_code=USDCNH.OTC"
headers1 = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Referer": "https://wallstreetcn.com/"
}

def test():
    contentx = "每天信息:最新汇率\n"
    response = requests.get(url=url1, headers=headers1)
    dict = json.loads(response.content.decode())
    symbol = dict["data"]["snapshot"]["USDCNH.OTC"][0]  # 美元
    prod_name = dict["data"]["snapshot"]["USDCNH.OTC"][1]  # 离岸人民币
    last_px = dict["data"]["snapshot"]["USDCNH.OTC"][2]  # 当前汇率
    px_change = dict["data"]["snapshot"]["USDCNH.OTC"][3]  # 涨跌额
    px_change_rate = dict["data"]["snapshot"]["USDCNH.OTC"][4]  # 涨跌幅
    trade = dict["data"]["snapshot"]["USDCNH.OTC"][16]  # 是否交易中

    contentx = contentx + " " + f"当前汇率：{last_px}\n" + f"涨 跌 幅：{px_change}\n" + f"涨 跌 幅：{px_change_rate}%"
    data = {
        "msgtype": "text",
        "text": {
            "content": contentx
        },
        "at": {
            "atMobiles": [
                info["phone"]
            ],
            "isAtAll": False
        }
    }

    # 如果在交易日，今天还没有发过信息，且涨幅或跌幅超过0.7%就发信息
    if trade == "TRADE" and px_change_rate > 0.7 or px_change_rate < 0.7:
        r = requests.post(url=url2,headers=headers2,data=json.dumps(data))
        print(r.json())


bSend = "2024-03-22"
while True:
    if bSend == datetime.date.today():
        time.sleep(300)
    else:
        test()
        bSend = datetime.date.today()


