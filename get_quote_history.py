#-*- coding: UTF-8 -*-

import efinance as ef

stock_code = '002927'
print(ef.stock.get_quote_history(stock_code))