from API.Helpers._Requests_ import API_req_creation
from API.Helpers._Web_Requests_ import M_SocketManager
from API.Constants.Coinbase_Con import Coinbase
#-----------------------------------------------
#-----------------------------------------------
# Calls related
#-----------------------------------------------
#-----------------------------------------------

class Coinbase_Functions(API_req_creation, Coinbase):

    def __init__(self, api_key = None, private_key = None):
        Coinbase.__init__(self, api_key, private_key)
        API_req_creation.__init__(self)
        self.session = self._init_session()

    def close(self):
        self.session.close()

    def product(self):

        return self._get('products')

    def product_ob(self, p_id, level = '1'):

        if level != '':
            level = '?' + str(level)

        return self._get('products/' + p_id + '/book' + level)

    def product_ticker(self, p_id):

        return self._get('products/' + p_id + '/ticker')

    def product_candles(self, p_id):

        return self._get('products/' + p_id + '/candles')

    def product_24h(self, p_id):

        return self._get('products/' + p_id + '/stats')

    def currencies(self):

        return self._get('currencies')

    def time(self):

        return self._get('time')

class Coinbase_Web_Functions(Coinbase, M_SocketManager):

    def __init__(self, api_key = None, private_key = None):
        Coinbase.__init__(self, api_key, private_key)
        M_SocketManager.__init__(self, self.STREAM_URL)

    def start_heartbeat(self, symbol, callback):
        data = {
            "type": "subscribe",
            "channels": {"name": "status", "product_ids": ["BTC-USD"]}
        }
        print(data)
        return self._start_socket("", callback, "", **{"payload":data})

    def start_status(self, callback):
        data = {
            "type": "subscribe",
            "channels": [
                {
                    "name": "status"
                }
            ]
        }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_ticker(self, symbol, callback):
        data = {
            "type": "subscribe",
            "channels": [
                {
                    "name": "level2",
                    "product_ids": [
                        symbol
                    ]
                }
            ]
        }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_matches(self, symbol, callback):
        data = {
            "type": "subscribe",
            "channels": [
                {
                    "name": "matches",
                    "product_ids": [
                        symbol
                    ]
                }
            ]
        }
        return self._start_socket("", callback, "", **{"payload":data})

    def start_full(self, symbol, callback):
        data = {
            "type": "subscribe",
            "channels": [
                {
                    "name": "full",
                    "product_ids": [
                        symbol
                    ]
                }
            ]
        }
        return self._start_socket("", callback, "", **{"payload":data})