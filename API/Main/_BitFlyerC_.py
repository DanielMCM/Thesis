from API.Helpers._Requests_ import API_req_creation
from API.Helpers._Web_Requests_ import M_SocketManager
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

    def history(self, symbol, count = 100, before = "", after = ""):
        p = {"params": {"product_code": symbol, "count": count}}
        if before != "":
            p["params"]["before"] = before
        if after != "":
            p["params"]["after"] = after

        return self._get('getexecutions', **p)

    def status(self, symbol):

         return self._get('gethealth', **{"params": {"product_code": symbol}})

     # Websockets

class BitFlyer_Web_Functions(BitFlyer, M_SocketManager):

    def __init__(self, api_key = None, private_key = None):
        BitFlyer.__init__(self, api_key, private_key)
        M_SocketManager.__init__(self, self.STREAM_URL)

    def start_book(self, symbol, callback):
        data = {'method' : 'subscribe',
            'params' : {'channel' : "lightning_board_snapshot_" + symbol}
            }
        print(data)
        return self._start_socket("", callback, "", **{"payload":data})

    def start_book_updates(self, symbol, callback):
        data = {'method' : 'subscribe',
            'params' : {'channel' : "lightning_board_" + symbol}
            }
        print(data)
        return self._start_socket("", callback, "", **{"payload":data})

    def start_ticker(self, symbol, callback):
        data = {'method' : 'subscribe',
            'params' : {'channel' : "lightning_ticker_" + symbol}
            }
        print(data)
        return self._start_socket("", callback, "", **{"payload":data})

    def start_executions(self, symbol, callback):
        data = {'method' : 'subscribe',
            'params' : {'channel' : "lightning_executions_" + symbol}
            }
        print(data)
        return self._start_socket("", callback, "", **{"payload":data})


                 