from Markets.API.Helpers._Requests_ import API_req_creation
from Markets.API.Constants.Binance_Con import Binance

#-----------------------------------------------
#-----------------------------------------------
# Calls related
#-----------------------------------------------
#-----------------------------------------------

class Binance_Functions(API_req_creation,Binance):
    #def __init__(self, api_key, api_secret):
    #    Binance.__init__(self, api_key, api_secret)

    def __init__(self, api_key = None, private_key = None):
        Binance.__init__(self, api_key, private_key)
        API_req_creation.__init__(self)
        self.session = self._init_session()
    
    def ping(self):

        return self._get('ping', version = 'v1')

    def get_server_time(self):

        return self._get('time', version = 'v1')

    def get_exchange_info(self):

        return self._get('exchangeInfo', version = 'v1')

    def depth(self, symbol, limit = 100):

        return self._get('depth', version = 'v1', **{"params": {"symbol": symbol, "limit":limit}})