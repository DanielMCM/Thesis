from API.Main.Client import Client, Web_Client
from datetime import datetime
import time

#a = Client.Bithumb()

#print("We get the first pair!\n")
#print(a.ticker("BTC_KRW"))

#print("We get the first pair!\n")
#print(a.config())

#print("We get the first pair!\n")
#print(a.order_book("BTC", "KRW"))

#print("We get the first pair!\n")
#print(a.History("BTC", "KRW"))

#print("We get the first pair!\n")
#print(a.index())

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
b.start_ticker('BTC-USDT', process_message)
b.start_order_book('BTC-USDT', process_message)
b.start_trade('BTC-USDT', process_message)

b.start()
#print(b.is_alive())
time.sleep(20)
b.close()