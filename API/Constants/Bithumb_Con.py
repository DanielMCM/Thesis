from API.Constants.General import Constant_values

class Bithumb(Constant_values):

    # Parameters used to connect with Bithumb

    # First we define the URLs
    API_URL = 'https://api.bithumb.com'

    STREAM_URL = "wss://global-api.bithumb.pro/message/realtime?subscribe="

    MARKET = "Bithumb"
