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

    def ticker(self, symbols):
        sym_list = ""
        for s in symbols:
            sym_list += s

        return self._get('tickers', **{"params": {"symbols": sym_list}})