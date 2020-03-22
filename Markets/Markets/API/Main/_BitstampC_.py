from Markets.API.Helpers._Requests_ import API_req_creation
from Markets.API.Helpers._Web_Requests_ import M_SocketManager
from Markets.API.Constants.Bitstamp_Con import Bitstamp

#-----------------------------------------------
#-----------------------------------------------
# Calls related
#-----------------------------------------------
#-----------------------------------------------

class Bitstamp_Functions(API_req_creation,Binance):
    #def __init__(self, api_key, api_secret):
    #    Binance.__init__(self, api_key, api_secret)

    def __init__(self, api_key = None, private_key = None):
        Bitstamp.__init__(self, api_key, private_key)
        API_req_creation.__init__(self)
        self.session = self._init_session()

    def close(self):
        self.session.close()

    def ticker(self, pair):

        return self._get('public/ticker/')