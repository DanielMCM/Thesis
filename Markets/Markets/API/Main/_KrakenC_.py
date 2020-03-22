from Markets.API.Helpers._Requests_ import API_req_creation
from Markets.API.Constants.Kraken_Con import Kraken

#-----------------------------------------------
#-----------------------------------------------
# Calls related
#-----------------------------------------------
#-----------------------------------------------

class Kraken_Functions(API_req_creation,Binance):
    #def __init__(self, api_key, api_secret):
    #    Binance.__init__(self, api_key, api_secret)

    def __init__(self, api_key = None, private_key = None):
        Kraken.__init__(self, api_key, private_key)
        API_req_creation.__init__(self)
        self.session = self._init_session()

    def close(self):
        self.session.close()

    def ticker(self):

        return self._get('0/public/Ticker')