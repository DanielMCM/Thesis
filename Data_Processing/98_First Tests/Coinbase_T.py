from API.Main.Client import Client, Web_Client
from datetime import datetime
import asyncio
#from copra.websocket import Channel, Client
import time


#a = Client.Coinbase()

#print("We get the products!\n")
#print(a.product())

#print("Now an order book")
#print(a.product_ob("BTC-USD", 1))

#print("Now an order book")
#print(a.product_ticker("BTC-USD"))

#print("Now an order book")
#print(a.product_candles("BTC-USD"))

#print("Now an order book")
#print(a.product_24h("BTC-USD"))

#print("Now an order book")
#print(a.currencies())

#print("Now an order book")
#print(a.time())

import cbpro, time
class myWebsocketClient(cbpro.WebsocketClient):
    def on_open(self):
        self.url = "wss://ws-feed.pro.coinbase.com/"
        self.products = ["LTC-USD"]
        self.channels = ["heartbeat"]
        self.message_count = 0
        print("Lets count the messages!")
    def on_message(self, msg):
        print(msg)
        self.message_count += 1
        if 'price' in msg and 'type' in msg:
            print ("Message type:", msg["type"],
                   "\t@ {:.3f}".format(float(msg["price"])))
    def on_close(self):
        print("-- Goodbye! --")

wsClient = myWebsocketClient()
wsClient.start()
print(wsClient.url, wsClient.products)
while (wsClient.message_count < 500):
    print ("\nmessage_count =", "{} \n".format(wsClient.message_count))
    time.sleep(1)
wsClient.close()