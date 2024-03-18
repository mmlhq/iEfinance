import math
import random
import json

def _get_market_code(stock_code):
    """
    根据股票代码计算出市场代码。
    :param stock_code: 股票代码
    :return: 市场代码，0：深圳，1：上海，2：其他
    """
    # 获取股票代码的前缀
    code_prefix = int(stock_code[:3])

    # 根据前缀判断市场
    if code_prefix in [0, 2, 3]:  # 深圳股票代码前缀一般为 0、2、3
        return 0  # 深圳市场
    elif code_prefix in [6, 9]:  # 上海股票代码前缀一般为 6、9
        return 1  # 上海市场
    else:
        return 2  # 其他市场，此处假设为北京市场


url = "https://" + str(math.floor(random.random()*99)+1) + ".push2.eastmoney.com/api/qt/stock/details/sse?fields1=f1,f2,f3,f4&fields2=f51,f52,f53,f54,f55&mpi=2000&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&pos=-0&secid=0.000006&wbp2u=3990134558939926|0|1|0|web"

params = {
    "fields1": "f1,f2,f3,f4",
    "fields2": "f51,f52,f53,f54,f55",
    "mpi": "2000",
    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
    "fltt": "2",
    "pos": "-0",
    "secid": "0.000004",
    "wbp2u": "3990134558939926|0|1|0|web"
}

import requests
from requests_sse import EventSource, InvalidStatusCodeError, InvalidContentTypeError

with EventSource(url,timeout=30) as event_source:
    try:
        for event in event_source:
            data = json.loads(event.data)
            print(data['data']['details'])
            break
    except InvalidStatusCodeError:
        pass
    except InvalidContentTypeError:
        pass
    except requests.RequestException:
        pass
