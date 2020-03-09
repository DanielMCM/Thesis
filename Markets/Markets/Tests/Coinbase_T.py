from Markets.Main.Client import Client
from datetime import datetime


a = Client.Coinbase()

print("We get the products!\n")
print(a.product())

print("Now an order book")
print(a.product_ob("BTC-USD", 1))