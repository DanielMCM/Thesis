from API.Helpers._Requests_ import API_req_creation
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