from API.Constants.General import Constant_values

class Coinbase(Constant_values):

    tld = "com"
    # First we define the URLs
    API_URL = 'https://api.pro.coinbase.{}'.format(tld)
    WITHDRAW_API_URL = ''
    MARGIN_API_URL = ''
    WEBSITE_URL = 'https://api.pro.coinbase.{}'.format(tld)
    FUTURES_URL = ''

    # Then API versions
    PUBLIC_API_VERSION = ''
    PRIVATE_API_VERSION = ''
    MARKET = 'Coinbase'

    STREAM_URL = "wss://ws-feed.gdax.com"