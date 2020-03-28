from API.Main.Client import Client, Web_Client
from datetime import datetime
import time

a = Client.Bitstamp()

print("We get the first pair!\n")
print(a.pairs_info())

print("We get the first pair!\n")
print(a.ticker("BTC", "USD"))

print("We get the first pair!\n")
print(a.hour_ticker("BTC", "USD"))

print("We get the first pair!\n")
print(a.order_book("BTC", "USD"))

print("We get the first pair!\n")
print(a.transactions("BTC", "USD"))

print("We get the first pair!\n")
print(a.eur_usd())

a.close()

def process_message(msg):
    try:
        print("message type: {}".format(msg['e']))
        print(msg)
    except:
        print("message type: Other")
        print(msg)

b = Web_Client.Bitstamp()
# start any sockets here, i.e a trade socket
b.start_ticker('btcusd', process_message)
b.start_orderBook('btcusd', process_message)
b.start_liveOrder('btcusd', process_message)
b.start_detailOrder('btcusd', process_message)
b.start_liveFull('btcusd', process_message)

b.start()
time.sleep(4)
b.close()
