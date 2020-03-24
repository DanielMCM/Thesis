from Markets.API.Helpers._Requests_ import API_req_creation
from Markets.API.Constants.Bitfinex_Con import Bitfinex

#-----------------------------------------------
#-----------------------------------------------
# Calls related
#-----------------------------------------------
#-----------------------------------------------

class Bitfinex_Functions(API_req_creation, Bithumb):

    def __init__(self, api_key = None, private_key = None):
        Bitfinex.__init__(self, api_key, private_key)
        API_req_creation.__init__(self)
        self.session = self._init_session()

    def close(self):
        self.session.close()

    def tickers(self, symbols):
        sym_list = ""
        for s in symbols:
            sym_list += s

        return self._get('tickers', **{"params": {"symbols": sym_list}})

    def tickers(self, symbol, limit = 120, start = "", end = "", sort = -1):

        return self._get('ticker/' + symbol, **{"params": {"limit": limit, "start": start, "end": end, "sort": sort}})

    def book(self, symbol, precision = "P0", len = 25):

        return self._get('book/' + symbol + "/" + precision, **{"params": {"len": len}})

    def status(self):

        return self._get('platform/status')

    def candles(self, symbol, timeframe = "1m", section = "last", period = "p30", limit = 120, start = "", end = "", sort = -1):

        return self._get('candles/trade:' + timeframe + ":" + symbol + ":" + period + "/" + section, **{"params": {"limit": limit, "start": start, "end": end, "sort": sort}})

    def configs(self, action = "list", Object = "pair", detail = ""):

        return self._get('conf/pub:' + action + ":" + Object + ":" + detail)

    def status(self, Type = "deriv", keys = "ALL", limit = 120, start = "", end = "", sort = -1):

        return self._get('status/' + Type, **{"params": {"keys": keys, "limit": limit, "start": start, "end": end, "sort": sort}})

    def liquidation(self, limit = 120, start = "", end = "", sort = -1):

        return self._get('liquidations/hist', **{"params": {"limit": limit, "start": start, "end": end, "sort": sort}})

    def leaderboards(self, Symbol, key = "vol", time_frame = "3h", Section = "hist", limit = 120, start = "", end = "", sort = -1):

        return self._get("rankings/" + Key + ":" + time_frame + ":" + Symbol + "/" + Section, **{"params": {"limit": limit, "start": start, "end": end, "sort": sort}})

    # POST NOT INCLUDED!!


    # WEBSOCKETS!



