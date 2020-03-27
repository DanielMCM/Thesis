from API.Main.Client import Client, Web_Client
from datetime import datetime
import time


a = Client.Coinbase()

print("We get the products!\n")
print(a.product())

print("Now an order book")
print(a.product_ob("BTC-USD", 1))