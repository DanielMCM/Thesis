from API.Helpers._Requests_ import API_req_creation
from API.Helpers._Web_Requests_ import M_SocketManager
from API.Constants.Huobi_Con import Huobi

#-----------------------------------------------
#-----------------------------------------------
# Calls related
#-----------------------------------------------
#-----------------------------------------------

class Huobi_Functions(API_req_creation,Huobi):
    #def __init__(self, api_key, api_secret):
    #    Binance.__init__(self, api_key, api_secret)

    def __init__(self, api_key = None, private_key = None):
        Huobi.__init__(self, api_key, private_key)
        API_req_creation.__init__(self)
        self.session = self._init_session()

    def close(self):
        self.session.close()

    def ticker(self):

        return self._get('market/tickers')

    def Kline(self, symbol, period = "1day", size = 200):

        return self._get('market/history/kline?symbol=' + symbol + '&period=' + period + '&size=' + str(size))

    def aggticker(self, symbol):

        return self._get('market/detail/merged?symbol=' + symbol)

    def depth(self, symbol, depth = 20, type = "step0"):

        return self._get('market/depth?symbol=' + symbol + '&type=' + type + '&depth=' + str(depth))

    def trade(self, symbol):

        return self._get('market/trade?symbol=' + symbol)

    def symbols(self):

        return self._get('common/symbols', version = "v1")

    def history(self, symbol, size = 1):

        return self._get('market/history/trade?symbol=' + symbol + "&size=" + str(size))

    def summary_24h(self, symbol):

        return self._get('market/detail?symbol=' + symbol)

    def currencies(self):

        return self._get('common/currencys', version = "v1")

    def time(self):

        return self._get('common/timestamp', version = "v1")

class Huobi_Web_Functions(Huobi, M_SocketManager):

    def __init__(self, api_key = None, private_key = None):
        Huobi.__init__(self, api_key, private_key)
        M_SocketManager.__init__(self, self.STREAM_URL)

    def start_candle(self, symbol, callback, period = "1min"):
        data = {
                  "sub": "market." + symbol + ".kline." + period,
                  "id": "id1"
                }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_depth(self, symbol, callback, type = "step0"):
        data = {
                  "sub": "market." + symbol + ".depth." + type,
                  "id": "id1"
                }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_refresh(self, symbol, callback, levels = 150):
        data = {
                  "sub": "market." + symbol + ".mbp.refresh." + str(levels),
                  "id": "id1"
                }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_by_price(self, symbol, callback, levels = 150):
        data = {
                  "sub": "market." + symbol + ".mbp." + str(levels),
                  "id": "id1"
                }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_bid_offer(self, symbol, callback, levels = 150):
        data = {
                  "sub": "market." + symbol + ".bbo",
                  "id": "id1"
                }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_24h(self, symbol, callback, levels = 150):
        data = {
                  "sub": "market." + symbol + ".detail",
                  "id": "id1"
                }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_trade(self, symbol, callback):
        data = {
                  "sub": "market." + symbol + ".trade.detail",
                  "id": "id1"
                }
        return self._start_socket("", callback, "", **{"payload":data})