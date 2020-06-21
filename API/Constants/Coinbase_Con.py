from API.Constants.General import Constant_values

class Coinbase(Constant_values):

    # Parameters used to connect with Coinbase

    tld = "com"
    # First we define the URLs
    API_URL = 'https://api.pro.coinbase.{}'.format(tld)

    # Then API versions
    PUBLIC_API_VERSION = ''
    PRIVATE_API_VERSION = ''
    MARKET = 'Coinbase'

    STREAM_URL = "wss://ws-feed.gdax.com"