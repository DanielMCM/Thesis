from API.Helpers._Requests_ import API_req_creation
from API.Helpers._Web_Requests_ import M_SocketManager
from API.Constants.Kraken_Con import Kraken

#-----------------------------------------------
#-----------------------------------------------
# Calls related
#-----------------------------------------------
#-----------------------------------------------

class Kraken_Functions(API_req_creation,Kraken):
    #def __init__(self, api_key, api_secret):
    #    Binance.__init__(self, api_key, api_secret)

    def __init__(self, api_key = None, private_key = None):
        Kraken.__init__(self, api_key, private_key)
        API_req_creation.__init__(self)
        self.session = self._init_session()

    def close(self):
        self.session.close()

    def ticker(self, symbol):

        return self._get('0/public/Ticker?pair=' + symbol)

    def time(self):

        return self._get('0/public/Time')

    def Assets(self):

        return self._get('0/public/Assets')

    def AssetPairs(self):

        return self._get('0/public/AssetPairs')

    def Depth(self, pair, count = "20"):

        return self._get('0/public/Depth?pair=' + pair + "&count=" + count)

    def OHLC(self, pair, interval = "1"):

        return self._get('0/public/OHLC?pair=' + pair + "&interval=" + interval)

    def Trades(self, pair):

        return self._get('0/public/Trades?pair=' + pair)

    def Spread(self, pair):

        return self._get('0/public/Spread?pair=' + pair)

class Kraken_Web_Functions(Kraken, M_SocketManager):

    def __init__(self, api_key = None, private_key = None):
        Kraken.__init__(self, api_key, private_key)
        M_SocketManager.__init__(self, self.STREAM_URL)

    def start_ticker(self, symbol, callback, levels = 150):
        data = {
                  "event": "subscribe",
                  "pair": [
                    symbol
                  ],
                  "subscription": {
                    "name": "ticker"
                  }
                }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_OHLC(self, symbol, callback):
        data = {
                  "event": "subscribe",
                  "pair": [
                    symbol
                  ],
                  "subscription": {
                    "name": "ohlc"
                  }
                }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_trade(self, symbol, callback):
        data = {
                  "event": "subscribe",
                  "pair": [
                    symbol
                  ],
                  "subscription": {
                    "name": "trade"
                  }
                }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_book(self, symbol, callback, depth = 500):

        data = {
                  "event": "subscribe",
                  "subscription": {
                    "depth": depth,
                    "name": "book"
                  },
                  "pair": [
                    symbol
                  ]
                }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_spread(self, symbol, callback):
        data = {
                  "event": "subscribe",
                  "pair": [
                    symbol
                  ],
                  "subscription": {
                    "name": "spread"
                  }
                }
        return self._start_socket("", callback, "", **{"payload":data})