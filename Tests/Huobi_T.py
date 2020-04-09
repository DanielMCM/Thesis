from API.Main.Client import Client, Web_Client
from datetime import datetime
import time

a = Client.Huobi()

print("We get the first pair!1\n")
print(a.close())

print("We get the first pair!2\n")
print(a.ticker())

print("We get the first pair!3 \n")
print(a.Kline("btcusdt"))

print("We get the first pair! 4\n")
print(a.aggticker("btcusdt"))

print("We get the first pair! 5\n")
print(a.depth("btcusdt"))

print("We get the first pair!6\n")
print(a.trade("btcusdt"))

print("We get the first pair!7\n")
print(a.symbols())

print("We get the first pair!8\n")
print(a.history("btcusdt"))

print("We get the first pair!9\n")
print(a.summary_24h("btcusdt"))

print("We get the first pair!10\n")
print(a.currencies())

print("We get the first pair!11\n")
print(a.time())


a.close()

def process_message(msg):
    try:
        print("message type: {}".format(msg['e']))
        print(msg)
    except:
        print("message type: Other")
        print(msg)

b = Web_Client.Huobi()
# start any sockets here, i.e a trade socket
#b.start_candle('btcusdt', process_message)
#b.start_by_price('btcusdt', process_message)
#b.start_depth('btcusdt', process_message)
#b.start_bid_offer('btcusdt', process_message)
#b.start_24h('btcusdt', process_message)
b.start_trade('btcusdt', process_message)



b.start()
#print(b.is_alive())
time.sleep(20)
b.close()

