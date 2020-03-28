from API.Main.Client import Client, Web_Client
from datetime import datetime
import time

#a = Client.Bithumb()

print("We get the first pair!\n")
print(a.ticker())

print("We get the first pair!\n")
print(a.kline("BTCUSD"))

print("We get the first pair!\n")
print(a.aggticker("BTCUSD"))

print("We get the first pair!\n")
print(a.depth("BTCUSD"))

print("We get the first pair!\n")
print(a.trade("BTCUSD"))

print("We get the first pair!\n")
print(a.symbols())

print("We get the first pair!\n")
print(a.history("BTCUSD"))

print("We get the first pair!\n")
print(a.summary_24h("BTCUSD"))

print("We get the first pair!\n")
print(a.currencies())

print("We get the first pair!\n")
print(a.time())


a.close()

def process_message(msg):
    try:
        print("message type: {}".format(msg['e']))
        print(msg)
    except:
        print("message type: Other")
        print(msg)

b = Web_Client.Bithumb()
# start any sockets here, i.e a trade socket
b.start_candle('BTC-USDT', process_message)
b.start_depth('BTC-USDT', process_message)
b.start_bid_offer('BTC-USDT', process_message)
b.start_24h('BTC-USDT', process_message)



b.start()
#print(b.is_alive())
time.sleep(20)
b.close()

