from API.Constants.Binance_Con import Binance
from API.Constants.Coinbase_Con import Coinbase
from API.Constants.General import Constant_values
from API.Helpers._Exceptions_ import BinanceAPIException
import time
import requests

#-----------------------------------------------
#-----------------------------------------------
# URL Creation
#-----------------------------------------------
#-----------------------------------------------

class API_req_creation():

    def _init_session(self):
        # Used sor starting a requests session
        session = requests.session()
        session.headers.update({'User-Agent': 'python/api', 'Accept-Encoding': 'gzip, deflate', 'Accept': '*/*', 'Connection': 'keep-alive'})

        return session

    def close(self):
        self.session.close()

    def _get(self, path, signed=False, version='', **kwargs):
        return self._request_api('get', path, signed, version, **kwargs)

    def _request_api(self, method, path, signed=False, version='', **kwargs):
        uri = self._create_api_uri(path, signed, version)

        return self._request(method, uri, signed, **kwargs)

    def _create_api_uri(self, path, signed, version=''):
        if version == '':
            v = '/' 
        else:
           v = '/' + version + '/'
        return self.API_URL + v + path


    def _request(self, method, uri, signed, force_params=False, **kwargs):

        if signed:
            # generate signature
            kwargs['data']['timestamp'] = int(time.time() * 1000)
            kwargs['data']['signature'] = self._generate_signature(kwargs['data'])
        #print(uri)
        #print(kwargs)
        self.response = getattr(self.session, method)(uri, **kwargs)
        #self.response = getattr(self.session, method)(uri, **kwargs)
        #print(self.response)
        return self._handle_response()


    def _generate_signature(self, data):

        ordered_data = self._order_params(data)
        query_string = '&'.join(["{}={}".format(d[0], d[1]) for d in ordered_data])
        m = hmac.new(self.API_SECRET.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256)
        return m.hexdigest()

    def _order_params(self, data):
        """Convert params to list with signature as last element
        :param data:
        :return:
        """
        has_signature = False
        params = []
        for key, value in data.items():
            if key == 'signature':
                has_signature = True
            else:
                params.append((key, value))
        # sort parameters by key
        params.sort(key=itemgetter(0))
        if has_signature:
            params.append(('signature', data['signature']))
        return params

    def _handle_response(self):
        """Internal helper for handling API responses from the Binance server.
        Raises the appropriate exceptions when necessary; otherwise, returns the
        response.
        """
        if not str(self.response.status_code).startswith('2'):
            raise BinanceAPIException(self.response)
        try:
            return self.response.json()
        except ValueError:
            raise BinanceRequestException('Invalid Response: %s' % self.response.text)