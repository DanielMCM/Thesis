from API.Main.Client import Client, Web_Client
from datetime import datetime
import time

a = Client.Kraken()

print("We get the first pair!\n")
print(a.ticker("ADAEUR"))

print("We get the first pair!\n")
print(a.time())

print("We get the first pair!\n")
print(a.Assets())

print("We get the first pair!\n")
print(a.AssetPairs())

print("We get the first pair!\n")
print(a.Depth("ADAEUR"))

print("We get the first pair!\n")
print(a.OHLC("ADAEUR"))

print("We get the first pair!\n")
print(a.Trades("ADAEUR"))

print("We get the first pair!\n")
print(a.Spread("ADAEUR"))

a.close()

def process_message(msg):
    try:
        print("message type: {}".format(msg['e']))
        print(msg)
    except:
        print("message type: Other")
        print(msg)

b = Web_Client.Kraken()
# start any sockets here, i.e a trade socket
b.start_ticker('BTC-USDT', process_message)
b.start_OHLC('BTC-USDT', process_message)
b.start_trade('BTC-USDT', process_message)
b.start_book('BTC-USDT', process_message)
b.start_spread('BTC-USDT', process_message)

b.start()
#print(b.is_alive())
time.sleep(20)
b.close()

