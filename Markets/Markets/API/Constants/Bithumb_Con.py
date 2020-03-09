from Markets.API.Constants.General import Constant_values

class Bithumb(Constant_values):

    tld = "com"
    # First we define the URLs
    API_URL = 'https://api.bithumb.{}/'.format(tld)

    MARKET = "Bithumb"
