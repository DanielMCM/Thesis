# coding=utf-8

import json
import threading

from autobahn.twisted.websocket import WebSocketClientFactory, \
    WebSocketClientProtocol, \
    connectWS
from twisted.internet import reactor, ssl
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.error import ReactorAlreadyRunning

from binance.client import Client


class M_ClientProtocol(WebSocketClientProtocol):

    def __init__(self, factory, payload=None):
        super(WebSocketClientProtocol, self).__init__()
        self.factory = factory
        self.payload = payload
        

    def onOpen(self):
        self.factory.protocol_instance = self

    def onConnect(self, response):
        if self.payload != "":
            self.sendMessage(self.payload, isBinary=False)
        # reset the delay after reconnecting
        self.factory.resetDelay()

    def onMessage(self, payload, isBinary):
        if not isBinary:
            try:
                payload_obj = json.loads(payload.decode('utf8'))
            except ValueError:
                pass
            else:
                self.factory.callback(payload_obj)




class M_ReconnectingClientFactory(ReconnectingClientFactory):

    # set initial delay to a short time
    initialDelay = 0.1

    maxDelay = 10

    maxRetries = 5


class M_ClientFactory(WebSocketClientFactory, M_ReconnectingClientFactory):

    def __init__(self, *args, payload=None, **kwargs):
        WebSocketClientFactory.__init__(self, *args, **kwargs)
        self.protocol_instance = None
        self.base_client = None
        self.payload = payload

    protocol = M_ClientProtocol

    _reconnect_error_payload = {
        'e': 'error',
        'm': 'Max reconnect retries reached'
    }

    def clientConnectionFailed(self, connector, reason):
        self.retry(connector)
        if self.retries > self.maxRetries:
            self.callback(self._reconnect_error_payload)

    def clientConnectionLost(self, connector, reason):
        self.retry(connector)
        if self.retries > self.maxRetries:
            self.callback(self._reconnect_error_payload)

    def buildProtocol(self, addr):
        return M_ClientProtocol(self, payload = self.payload)


class M_SocketManager(threading.Thread):

    WEBSOCKET_DEPTH_5 = '5'
    WEBSOCKET_DEPTH_10 = '10'
    WEBSOCKET_DEPTH_20 = '20'

    DEFAULT_USER_TIMEOUT = 30 * 60  # 30 minutes

    def __init__(self, STREAM_URL, user_timeout=DEFAULT_USER_TIMEOUT):

        threading.Thread.__init__(self)
        self._conns = {}
        #self._client = client
        self._user_timeout = user_timeout
        self._timers = {'user': None, 'margin': None}
        self._listen_keys = {'user': None, 'margin': None}
        self._account_callbacks = {'user': None, 'margin': None}

    def _start_socket(self, path, callback, prefix='', **Kwargs):
        payload = ""
        if "payload" in Kwargs:
            payload = json.dumps(Kwargs["payload"], ensure_ascii=False).encode('utf8')
        #if path in self._conns:
        #    return False
        factory_url = self.STREAM_URL + prefix + path
        factory = M_ClientFactory(factory_url, payload = payload)
        factory.protocol = M_ClientProtocol
        factory.callback = callback
        factory.reconnect = True
        factory.params
        context_factory = ssl.ClientContextFactory()

        self._conns[path] = connectWS(factory, context_factory)
        return path

    def stop_socket(self, conn_key):
        """Stop a websocket given the connection key
        :param conn_key: Socket connection key
        :type conn_key: string
        :returns: connection key string if successful, False otherwise
        """
        if conn_key not in self._conns:
            return

        # disable reconnecting if we are closing
        self._conns[conn_key].factory = WebSocketClientFactory(self.STREAM_URL + 'tmp_path')
        self._conns[conn_key].disconnect()
        del(self._conns[conn_key])

        # check if we have a user stream socket
        if len(conn_key) >= 60 and conn_key[:60] == self._listen_keys['user']:
            self._stop_account_socket('user')

        # or a margin stream socket
        if len(conn_key) >= 60 and conn_key[:60] == self._listen_keys['margin']:
            self._stop_account_socket('margin')

    def _stop_account_socket(self, socket_type):
        if not self._listen_keys[socket_type]:
            return
        if self._timers[socket_type]:
            self._timers[socket_type].cancel()
            self._timers[socket_type] = None
        self._listen_keys[socket_type] = None

    def run(self):
        try:
            reactor.run(installSignalHandlers=False)
        except ReactorAlreadyRunning:
            # Ignore error about reactor already running
            pass

    def close(self):
        """Close all connections
        """
        keys = set(self._conns.keys())
        for key in keys:
            self.stop_socket(key)

        self._conns = {}
        try:
            reactor.stop()
        except:
            pass