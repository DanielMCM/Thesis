from API.Helpers._Requests_ import API_req_creation
from API.Helpers._Web_Requests_ import M_SocketManager
from API.Constants.Bitfinex_Con import Bitfinex

#-----------------------------------------------
#-----------------------------------------------
# Calls related
#-----------------------------------------------
#-----------------------------------------------

class Bitfinex_Functions(API_req_creation, Bitfinex):

    def __init__(self, api_key = None, private_key = None):
        Bitfinex.__init__(self, api_key, private_key)
        API_req_creation.__init__(self)
        self.session = self._init_session()

    def close(self):
        self.session.close()

    def tickers(self, symbol):
        sym_list = symbol[0]
        for s in symbol[1::]:
            sym_list = sym_list + "," + s
        print(sym_list)
        return self._get('tickers',version = "v2", **{"params": {"symbols": sym_list}})

    def ticker(self, symbol, limit = 120, start = "", end = "", sort = -1):

        return self._get('ticker/' + symbol,version = "v2", **{"params": {"limit": limit, "start": start, "end": end, "sort": sort}})

    def book(self, symbol, precision = "P0", len = 25):

        return self._get('book/' + symbol + "/" + precision,version = "v2", **{"params": {"len": len}})

    def status(self):

        return self._get('status/deriv', version= "v2")

    def candles(self, symbol, timeframe = "1m", section = "last", period = "p30", limit = 120, start = "", end = "", sort = -1):

        return self._get('candles/trade:' + timeframe + ":" + symbol + ":" + period + "/" + section,version = "v2", **{"params": {"limit": limit, "start": start, "end": end, "sort": sort}})

    def configs(self, action = "list", Object = "pair", detail = "exchange"):

        return self._get('conf/pub:' + action + ":" + Object + ":" + detail, version="v2")

    def status_2(self, Type = "deriv", keys = "ALL", limit = 120, start = "", end = "", sort = -1):

        return self._get('status/' + Type,version = "v2", **{"params": {"keys": keys, "limit": limit, "start": start, "end": end, "sort": sort}})

    def liquidation(self, limit = 120, start = "", end = "", sort = -1):

        return self._get('liquidations/hist',version = "v2", **{"params": {"limit": limit, "start": start, "end": end, "sort": sort}})

    def leaderboards(self, Symbol, key = "vol", time_frame = "3h", Section = "hist", limit = 120, start = "", end = "", sort = -1):

        return self._get("rankings/" + key + ":" + time_frame + ":" + Symbol + "/" + Section,version = "v2", **{"params": {"limit": limit, "start": start, "end": end, "sort": sort}})

    def order_2(self, Symbol):

        return self._get("book/" + Symbol, version = "v1")


    # POST NOT INCLUDED!!


    # WEBSOCKETS!

class Bitfinex_Web_Functions(Bitfinex, M_SocketManager):

    def __init__(self, api_key = None, private_key = None):
        Bitfinex.__init__(self, api_key, private_key)
        M_SocketManager.__init__(self, self.STREAM_URL)

    def start_ticker(self, symbol, callback):
        data = {
            'event': 'subscribe',
            'channel': 'ticker',
            'symbol': symbol,
        }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_trades(self, symbol, callback):
        data = {
            'event': 'subscribe',
            'channel': 'trades',
            'symbol': symbol,
        }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_book(self, symbol, callback, precision = "P0", frequency = "F0", length = 25):
        data = {
            'event': 'subscribe',
            'channel': 'book',v1
            'symbol': symbol,
            'precision': precision, 
            'frequency': frequency, 
            'length': length
        }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_raw_book(self, symbol, callback, precision = "R0", length = 100):
        data = {
            'event': 'subscribe',
            'channel': 'book',
            'symbol': symbol,
            'prec': precision, 
            'len': length
        }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_candles(self, symbol, callback,  timeframe = "1m"):
        data = {
            'event': 'subscribe',
            'channel': 'candles',
            'key': 'trade:' + timeframe + ":" + symbol
        }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_status(self, callback, symbol="global", type = "liq"):
        data = {
            'event': 'subscribe',
            'channel': 'book',
            'key': type + ":" + symbol
        }
        return self._start_socket("", callback, "", **{"payload":data})
