from Markets.API.Helpers._Requests_ import API_req_creation
from Markets.API.Constants.Bithumb_Con import Bithumb

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

    def ticker(self, first, second):

        return self._get('public/ticker/' + first + "_" + second)

