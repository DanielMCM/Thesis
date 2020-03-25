from API.Constants.General import Constant_values

class Bitfinex(Constant_values):

    # First we define the URLs
    API_URL = 'https://api-pub.bitfinex.com'

    # Authenticated https://api.bitfinex.com

    MARKET = "Bitfinex"

    STREAM_URL = "wss://api-pub.bitfinex.com/ws/2/"

    # Authenticated wss://api.bitfinex.com/ws/2