from API.Constants.General import Constant_values

class Bithumb(Constant_values):

    tld = "com"
    # First we define the URLs
    API_URL = 'https://api.bithumb.{}'.format(tld)

    STREAM_URL = "wss://global-api.bithumb.pro/message/realtime?subscribe="

    MARKET = "Bithumb"