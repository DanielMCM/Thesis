from API.Helpers._Requests_ import API_req_creation
from API.Constants.BitFlyer_Con import BitFlyer

#-----------------------------------------------
#-----------------------------------------------
# Calls related
#-----------------------------------------------
#-----------------------------------------------

class BitFlyer_Functions(API_req_creation,BitFlyer):
    #def __init__(self, api_key, api_secret):
    #    Binance.__init__(self, api_key, api_secret)

    def __init__(self, api_key = None, private_key = None):
        BitFlyer.__init__(self, api_key, private_key)
        API_req_creation.__init__(self)
        self.session = self._init_session()

    def close(self):
        self.session.close()

    def ticker(self, symbol):

        return self._get('ticker', **{"params": {"product_code": symbol}})

    def markets(self):

        return self._get('getmarkets')

    def book(self, symbol):

        return self._get('getboard', **{"params": {"product_code": symbol}})

    def history(self, symbol, count, before, after):

        return self._get('getexecutions', **{"params": {"product_code": symbol, "count": count, "before": before, "after":after}})

    def status(self, symbol):

         return self._get('gethealth', **{"params": {"product_code": symbol}})

     # Websockets