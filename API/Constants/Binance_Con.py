from API.Constants.General import Constant_values

class Binance(Constant_values):

    tld = "com"
    # First we define the URLs
    API_URL = 'https://api.binance.{}/api'.format(tld)
    WITHDRAW_API_URL = 'https://api.binance.{}/wapi'.format(tld)
    MARGIN_API_URL = 'https://api.binance.{}/sapi'.format(tld)
    WEBSITE_URL = 'https://www.binance.{}'.format(tld)
    FUTURES_URL = 'https://fapi.binance.{}/fapi'.format(tld)

    STREAM_URL = 'wss://stream.binance.com:9443/'

    PUBLIC_API_VERSION = 'v1'
    PRIVATE_API_VERSION = 'v3'
    MARKET = "Binance"

