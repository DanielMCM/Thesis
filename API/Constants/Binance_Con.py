from API.Constants.General import Constant_values

class Binance(Constant_values):

    # Parameters used to connect with Binance

    tld = "com"
    # First we define the URLs
    API_URL = 'https://api.binance.{}/api'.format(tld)

    STREAM_URL = 'wss://stream.binance.com:9443/'

    PUBLIC_API_VERSION = 'v1'
    PRIVATE_API_VERSION = 'v3'
    MARKET = "Binance"

