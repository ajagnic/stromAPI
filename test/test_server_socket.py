import unittest
import requests
import time
from threading import Thread
from .server import app, socketio
from strom.coordinator.coordinator import Coordinator
from strom.utils.configer import configer as config


class ServerThread(Thread):
    def __init__(self):
        super().__init__()
        self.app = app
        self.sock = socketio

    def run(self):
        self.sock.run(self.app)

def post_events(events):
   endpoint = 'http://{}:{}/new_event'.format(config['server_host'], config['server_port'])
   r = requests.post(endpoint, json=events)
   return r

def terminate_server():
   endpoint = 'http://{}:{}/terminate'.format(config['server_host'], config['server_port'])
   r = requests.get(endpoint)
   return r


class TestServerSocket(unittest.TestCase):
    def setUp(self):
        self.dir = "demo_data/"
        self.url = "http://127.0.0.1:5000"
        self.server = ServerThread()
        self.coordinator = Coordinator()
        self.dum = {"engine_rules": {"kafka": "kody"}, "events": {"lucy_on_couch": [{"yikes":"Oh_NO"}, {"getoverhere":True, "notreats":"Why not"}]}, "lucky_napping": ["awww", "sleepy", "sleepwoof"]}
        self.dummy = {"event": "lucy_on_couch_kody", "data": [{"yikes":"Oh_NO"}, {"getoverhere":True, "notreats":"Why not"}]}

    def test_handle_event_detection(self):


        self.server.start()

        #
        time.sleep(5)
        r = self.coordinator._post_events(self.dummy)
        print("post return", r)
        time.sleep(5)

        self.assertTrue(self.server.sock.check_msg)
        terminate_server()

    def test_test_parse(self):
        result = self.coordinator._parse_events(self.dum)
        self.assertIn({"event": "lucy_on_couch_kody", "data": [{"yikes":"Oh_NO"}, {"getoverhere":True, "notreats":"Why not"}]}, result)

