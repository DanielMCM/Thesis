from API.Helpers._Requests_ import API_req_creation
from API.Helpers._Web_Requests_ import M_SocketManager
from API.Constants.Bithumb_Con import Bithumb

#-----------------------------------------------
#-----------------------------------------------
# Calls related
#-----------------------------------------------
#-----------------------------------------------

class Bithumb_Functions(API_req_creation, Bithumb):

    def __init__(self, api_key = None, private_key = None):
        Bithumb.__init__(self, api_key, private_key)
        API_req_creation.__init__(self)
        self.session = self._init_session()

    def close(self):
        self.session.close()

    def config(self, type = 'config'):

        return self._get('spot/' + type)

    def ticker(self, symbol):

        return self._get('public/ticker/' + symbol)

    def order_book(self, first, second):

        return self._get('public/orderbook/' + first + "_" + second)

    def History(self, first, second):

        return self._get('public/transaction_history/' + first + "_" + second)

    def index(self):

        return self._get('public/btci')

class Bithumb_Web_Functions(Bithumb, M_SocketManager):

    def __init__(self, api_key = None, private_key = None):
        Bithumb.__init__(self, api_key, private_key)
        M_SocketManager.__init__(self, self.STREAM_URL)

    def start_ticker(self, symbol, callback):

        return self._start_socket("TICKER:" + symbol, callback)

    def start_order_book(self, symbol, callback):

        return self._start_socket("ORDERBOOK:" + symbol, callback)

    def start_trade(self, symbol, callback):

        return self._start_socket("TRADE:" + symbol, callback)
