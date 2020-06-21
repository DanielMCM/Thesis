# cbpro/WebsocketClient.py
# original author: Daniel Paquin
# mongo "support" added by Drew Rice
#
#
# Template object to receive messages from the Coinbase Websocket Feed

from __future__ import print_function
import json
#import base64
#import hmac
#import hashlib
import time
#import ast
from threading import Lock, Thread, Event, currentThread

from websocket import create_connection, WebSocketConnectionClosedException
import gzip
from random import random

#-----------------------------------------------
#-----------------------------------------------
# Websocket requests 
#-----------------------------------------------
#-----------------------------------------------

class M_SocketManager(object):
    def __init__(self, url):
        self.url = url
        self.cont = True
        self.error = None
        self.ws = None
        self.thread = []
        self.kw = 0

    def connect_to(self, path, callback, payload = ""):

        pingsend3 = "wss://api.bitfinex" in path
        bitsig = 0
        try:
            ws_a = create_connection(path)
            if pingsend3 and bitsig == 0:
                bitsig = 1
                data = {"event": "conf", "flags": 32768}
                data = json.dumps(data).encode()
                ws_a.send(data)
            if payload != "":
                ws_a.send(payload)
        except Exception as e:
            print("Connection not created!!")
            print(path)
            print(e)
            print("Reconnecting...")
            time.sleep(2)
            self.connect_to(path, callback, payload)
            return

        t = currentThread()
        
        bittime = time.time()
        pingsend = "wss://global-api.bithumb.pro" in path
        pingsend2 = "wss://ws.kraken.com" in path
        
        while getattr(t, "do_run", True): 
            try:
                if  pingsend and time.time()-bittime > 5:
                    bittime = time.time()
                    data = {"cmd":"ping"}
                    data = json.dumps(data).encode()
                    ws_a.send(data)
                elif pingsend2 and time.time() - bittime > 30:
                    bittime = time.time()
                    data = {"event": "ping", "reqid": 42}
                    data = json.dumps(data).encode()
                    ws_a.send(data)
                pre_msg =  ws_a.recv()
                msg = json.loads(pre_msg)
                callback(msg)
            except (WebSocketConnectionClosedException, ConnectionResetError, ConnectionAbortedError, ConnectionRefusedError) as e:
                #print("\n" + str(e))
                print("\n Reconnecting " + path + "... \n")
                if "bitstamp" not in path:
                    print(e)
                    time.sleep(random()*5)
                else:
                    time.sleep(2)
                self.connect_to(path, callback, payload)
                return
            except Exception as e:
                try:
                    msg = json.loads(gzip.decompress(pre_msg).decode())
                    if 'ping' in msg:
                        data = {
                            "pong": msg['ping'] 
                        }
                        data = json.dumps(data).encode()
                        ws_a.send(data)
                    callback(msg)
                except TypeError:
                    if pre_msg == "" or msg == "":
                        pass
                except Exception as e2:
                    print("Chain of errors, first error:")
                    print(type(e))
                    print(e.args)
                    print(e)
                    print("Second error:")
                    print(type(e2))
                    print(e2.args)
                    print(e2)
                    print(pre_msg)
                    print("RECONNECTING")
                    print(msg)
                    print(type(msg))
                    print(msg == "")
                    ws_a.close()
            msg = ""
            pre_msg = ""

    def _start_socket(self, path, callback, version = "", prefix = "", **Kwargs):
        self.kw = Kwargs
        if self.url not in path:
            con = self.url
            if version != "":
                con += version
            if prefix != "":
                con += prefix
            if path != "":
                con += path
        else:
            con = path
        payload = ""
        if "payload" in Kwargs:
            payload = json.dumps(Kwargs["payload"], ensure_ascii=False).encode('utf8')

        thr = Thread(target=self.connect_to, args=(con, callback, payload))
        thr.setDaemon(True)
        self.thread.append(thr)

    def start(self):
        for th in self.thread:
            th.start()

    def _disconnect(self):
        try:
            if self.ws:
                self.ws.close()
        except WebSocketConnectionClosedException as e:
            pass

        self.on_close()
    def on_close(self):
        print("\n-- Socket Closed --")

    def close(self):
        for th in self.thread:
            th.do_run = False