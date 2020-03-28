from API.Helpers._Requests_ import API_req_creation
from API.Helpers._Web_Requests_ import M_SocketManager
from API.Constants.Bitstamp_Con import Bitstamp

#-----------------------------------------------
#-----------------------------------------------
# Calls related
#-----------------------------------------------
#-----------------------------------------------

class Bitstamp_Functions(API_req_creation,Bitstamp):
    #def __init__(self, api_key, api_secret):
    #    Binance.__init__(self, api_key, api_secret)

    def __init__(self, api_key = None, private_key = None):
        Bitstamp.__init__(self, api_key, private_key)
        API_req_creation.__init__(self)
        self.session = self._init_session()

    def close(self):
        self.session.close()

    def ticker(self, first, second):

        return self._get('ticker/' + first.lower() + second.lower(), version = "v2")

    def hour_ticker(self, first, second):

        return self._get('ticker_hour/' + first.lower() + second.lower(), version = "v2")

    def order_book(self, first, second):

        return self._get('order_book/' + first.lower() + second.lower(), version = "v2")

    def transactions(self, first, second):

        return self._get('transactions/' + first.lower() + second.lower(), version = "v2")

    def pairs_info(self):

        return self._get('trading-pairs-info/', version = "v2")

    def eur_usd(self):

        return self._get('eur_usd/')

class Bitstamp_Web_Functions(Bitstamp, M_SocketManager):

    def __init__(self, api_key = None, private_key = None):
        Bitstamp.__init__(self, api_key, private_key)
        M_SocketManager.__init__(self, self.STREAM_URL)

    def start_ticker(self, symbol, callback):
        data = {
                "event": "bts:subscribe",
                "data": {
                    "channel": "live_trades_" + symbol
                }
        }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_orderBook(self, symbol, callback):
        data = {
                "event": "bts:subscribe",
                "data": {
                    "channel": "order_book_" + symbol
                }
        }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_liveOrder(self, symbol, callback):
        data = {
                "event": "bts:subscribe",
                "data": {
                    "channel": "live_orders_" + symbol
                }
        }
        
        return self._start_socket("", callback, "", **{"payload":data})

    def start_detailOrder(self, symbol, callback):
        data = {
                "event": "bts:subscribe",
                "data": {
                    "channel": "detail_order_book_" + symbol
                }
        }
        
        return self._start_socket("", callback, "", **{"payload":data})

    def start_liveFull(self, symbol, callback):
        data = {
                "event": "bts:subscribe",
                "data": {
                    "channel": "diff_order_book_" + symbol
                }
        }
        
        return self._start_socket("", callback, "", **{"payload":data})