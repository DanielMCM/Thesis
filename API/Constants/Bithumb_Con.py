from API.Constants.General import Constant_values

class Bithumb(Constant_values):

    tld = "com"
    # First we define the URLs
    API_URL = 'https://global-openapi.bithumb.pro/openapi/v1'

    STREAM_URL = "wss://global-api.bithumb.pro/message/realtime?subscribe="

    MARKET = "Bithumb"
