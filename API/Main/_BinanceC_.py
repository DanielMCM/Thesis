from API.Helpers._Requests_ import API_req_creation
from API.Helpers._Web_Requests_ import M_SocketManager
from API.Constants.Binance_Con import Binance

#-----------------------------------------------
#-----------------------------------------------
# Calls related
#-----------------------------------------------
#-----------------------------------------------

class Binance_Functions(API_req_creation,Binance):

    def __init__(self, api_key = None, private_key = None):
        Binance.__init__(self, api_key, private_key)
        API_req_creation.__init__(self)
        self.session = self._init_session()
    
    def ping(self):

        return self._get('ping', version = 'v3')

    def get_server_time(self):

        return self._get('time', version = 'v3')

    def get_exchange_info(self):

        return self._get('exchangeInfo', version = 'v3')

    def depth(self, symbol, limit = 100):

        return self._get('depth', version = 'v3', **{"params": {"symbol": symbol, "limit":limit}})

    def trades(self, symbol, limit = 500):

        return self._get('trades', version = 'v3', **{"params": {"symbol": symbol, "limit":limit}})

    def htrades(self, symbol, limit = 500, fromId = ""):
        if fromId == "":
            p = {"params": {"symbol": symbol, "limit":limit}}
        else:
            p = {"params": {"symbol": symbol, "limit":limit, "fromId": fromId}}

        return self._get('historicalTrades', version = 'v3', **p)

    def aggtrades(self, symbol, fromId = "", startTime = "", endTime = "", limit = 500):
        p = {"params": {"symbol": symbol, "limit": limit}}
        if fromId != "":
            p["params"]["fromID"] = fromId
        if startTime != "":
            p["params"]["startTime"] = startTime
        if endTime != "":
            p["params"]["endTime"] = endTime

        return self._get('aggTrades', version = 'v3', **p)

    def klines(self, symbol, interval, startTime = "", endTime = "", limit = 500):
        p = {"params": {"symbol": symbol, "interval": interval, "limit": limit}}
        if startTime != "":
            p["params"]["startTime"] = startTime
        if endTime != "":
            p["params"]["endTime"] = endTime
        return self._get('klines', version = 'v3', **p)

    def avgPrice(self, symbol):

        return self._get('avgPrice', version = 'v3', **{"params": {"symbol": symbol}})

    
    def tkr24(self, symbol):

        return self._get('ticker/24hr', version = 'v3', **{"params": {"symbol": symbol}})

    def price(self, symbol):

        return self._get('ticker/price', version = 'v3', **{"params": {"symbol": symbol}})

    def bookTicker(self, symbol):

        return self._get('ticker/bookTicker', version = 'v3', **{"params": {"symbol": symbol}})

class Binance_Web_Functions(Binance, M_SocketManager):

    def __init__(self, api_key = None, private_key = None):
        Binance.__init__(self, api_key, private_key)
        M_SocketManager.__init__(self, self.STREAM_URL)

    def start_trade_socket(self, symbol, callback):
        return self._start_socket(symbol.lower() + '@trade', callback, prefix="ws/")

    def start_aggtrade_socket(self, symbol, callback):
        return self._start_socket(symbol.lower() + '@aggTrade', callback, prefix="ws/")

    def start_kline_socket(self, symbol, callback, interval="1m"):
        socket_name = '{}@kline_{}'.format(symbol.lower(), interval)
        return self._start_socket(socket_name, callback, prefix="ws/")

    def start_miniticker_socket(self, symbol, callback, update_time=1000):
        return self._start_socket(symbol.lower() + '@arr@{}ms'.format(update_time), callback, prefix="ws/")

    def start_symbol_ticker_socket(self, symbol, callback):
        return self._start_socket(symbol.lower() + '@ticker', callback, prefix="ws/")

    def start_ticker_socket(self, callback):
        return self._start_socket('!ticker@arr', callback, prefix="ws/")

    def start_symbol_book_ticker_socket(self, symbol, callback):
        return self._start_socket(symbol.lower() + '@bookTicker', callback, prefix="ws/")

    def start_book_ticker_socket(self, callback):
        return self._start_socket('!bookTicker', callback, prefix="ws/")

    def start_depth_socket(self, symbol, callback, depth=None):
        socket_name = symbol.lower() + '@depth'
        if depth and depth != '1':
            socket_name = '{}{}'.format(socket_name, depth)
        return self._start_socket(socket_name, callback, prefix="ws/")

    def start_diff_depth_socket(self, symbol, callback, depth=None):
        socket_name = symbol.lower() + '@depth@100ms'
        return self._start_socket(socket_name, callback, prefix="ws/")