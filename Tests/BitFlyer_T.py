from API.Main.Client import Client, Web_Client

from datetime import datetime
import time
from twisted.internet import reactor

api_key = ""
secret_key = ""

#cl = Client.BitFlyer(api_key, secret_key)

#print("TICKER\n")
#print(cl.ticker(["BTC_JPY"]))

#print("MARKETS\n")
#print(cl.markets())

#print("BOOK\n")
#print(cl.book(["BTC_JPY"]))

#print("HISTORY\n")
#print(cl.history(["BTC_JPY"]))

#print("STATUS\n")
#print(cl.status(["BTC_JPY"]))


# MARKETPLACES

def process_message(msg):
    try:
        print("message type: {}".format(msg['e']))
        print(msg)
    except:
        print("message type: Other")
        print(msg)

    # do something

b = Web_Client.BitFlyer()
# start any sockets here, i.e a trade socket
b.start_book('BTC_JPY', process_message)
b.start_book_updates('BTC_JPY', process_message)
b.start_ticker('BTC_JPY', process_message)
b.start_executions('BTC_JPY', process_message)

b.start()
#print(b.is_alive())
time.sleep(20)
b.close()
#b.close()