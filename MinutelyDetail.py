# 爬取东财网分时数据
import json
import requests
import math
import random
from loguru import logger
import pymysql
import datetime
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine

def _get_market_code(stock_code):
    """
    根据股票代码计算出市场代码。
    :param stock_code: 股票代码
    :return: 市场代码，0：深圳，1：上海，2：其他
    """
    # 获取股票代码的前缀
    code_prefix = int(stock_code[:1])

    # 根据前缀判断市场
    if code_prefix in [0, 2, 3]:  # 深圳股票代码前缀一般为 0、2、3
        return 0  # 深圳市场
    elif code_prefix in [6, 9]:  # 上海股票代码前缀一般为 6、9
        return 1  # 上海市场
    else:
        return 2  # 其他市场，此处假设为北京市场


def _handle_event(event_data) -> str:
    """
    解析event数据
    :param event_data: SSE数据
    :return: 事件的数据字段
    """
    lines = event_data.strip().split('\n')
    data = ""

    for line in lines:
        if line.startswith("event:"):  # 东财分时数据的事件无event
            event_type = line.replace("event:", "").strip()
        elif line.startswith("data:"):
            data = line.replace("data:", "").strip()
    # logger.info(f"data received: {data}")
    return data


def get_minutely_data(code: str) -> dict:
    """
    获取最新的分时数据
    :param code: 股票代码，6位数字格式，比如：“000001”。
    :return: 数据字典
    """
    # url格式，需要调用者依次填充：市场代码（0：深圳，0：上海），股票代码，日期窗口
    url_format = "https://" + str(math.floor(random.random() * 99) + 1) + \
                 ".push2.eastmoney.com/api/qt/stock/trends2/sse?fields1=f1,f2,f3,f4,f5,f6,f7,f8," \
                 "f9,f10,f11,f12,f13,f14,f17&fields2=f51,f52,f53,f54,f55,f56,f57," \
                 "f58&mpi=1000&ut=fa5fd1943c7b386f172d6893dbfba10b&secid={market}.{code}" \
                 "&ndays={days}&iscr=0&iscca=0&wbp2u=1849325530509956|0|1|0|web"

    url = url_format.format(market=_get_market_code(code), code=code, days=1)  # 请求url
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Connection": "keep-alive",
        "Accept": "text/event-stream"
    }

    response = requests.get(url, headers=headers, stream=True)  # 请求数据，开启流式传输

    if response.status_code == 200:
        event_data = ""
        for chunk in response.iter_content(chunk_size=None):  # 不断地获取数据
            data_decoded = chunk.decode('utf-8')  # 将字节流解码成字符串
            event_data += data_decoded
            parts = event_data.split('\n\n')
            if len(parts) > 1:  # 获取到一条完整的事件后，对数据进行解析。
                data = _handle_event(parts[0])
                return json.loads(data)

def data_to_data_frame(data: dict) -> pd.DataFrame:
    try:
        trends = data["data"]["trends"]
        trends_str = json.dumps(trends)
        trend_csv_content = 'Time,Open,Close,High,Low,Volume,Amount,Average\n'
        trend_csv_content += trends_str.replace("\", \"", "\n").strip("\"[]")
        # logger.info(f"trends_str:\n{trend_csv_content}")

        type_mapping = {
            "Open": float,
            "High": float,
            "Low": float,
            "Close": float,
            "Volume": float,
            "Amount": float,
            "Average": float
        }
        df_data = pd.read_csv(
            StringIO(trend_csv_content), delimiter=',', dtype=type_mapping,
            parse_dates=['Time'], index_col='Time')
        # logger.info(f"df_data: {df_data}")
        return df_data
    except Exception as e:
        logger.error(f"异常：{e}")
        return None

if __name__ == "__main__":  # 测试代码

    with open("config/config.json", encoding="utf-8") as f:
        cfg = json.load(f)
    info = cfg["mysql"]
    cnx = pymysql.connect(user=info["user"], password=info["password"], host=info["host"], database=info["database"])
    cur_indexs = cnx.cursor()
    index_sql = "select code from tdx.vindex_b;"
    cur_indexs.execute(index_sql)
    tdx_indexs = cur_indexs.fetchall()

    date = datetime.date.today()

    cur_minute = cnx.cursor()
    for tindex in tdx_indexs:
        print(f"开始时间:{datetime.datetime.now()}")
        code = tindex[0]
        datas = get_minutely_data(code)
        cells = datas['data']['trends']
        print(f"正在写入:{code}")
        for cell in cells:
            rdatetime = cell.split(",")[0]
            date = rdatetime.split(" ")[0]
            time = rdatetime.split(" ")[1]
            open = cell.split(',')[1]
            high = cell.split(',')[2]
            low = cell.split(',')[3]
            close = cell.split(',')[4]
            volume = cell.split(',')[5]
            amount = cell.split(',')[6]
            average = cell.split(',')[7]

            cur_minute_sql = f"insert into MinutelyData(`code`,`date`,`time`,`open`,`high`,`low`,`close`,`volume`,`amount`,`average`) values('{code}','{date}','{time}',{open},{high},{low},{close},{volume},{amount},{average}) "
            cur_minute.execute(cur_minute_sql)

        cnx.commit()


    cur_minute.close()
    cur_indexs.close()
    cnx.close()
    print(f"结束时间:{datetime.datetime.now()}")