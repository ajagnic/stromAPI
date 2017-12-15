import unittest
import requests
import time
from threading import Thread
from .server import app, socketio
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


class TestServer(unittest.TestCase):
    def setUp(self):
        self.dir = "demo_data/"
        self.url = "http://127.0.0.1:5000"
        self.server = ServerThread()

    def test_handle_event_detection(self):


        self.server.start()

        dummy_data = {"key": "why is this not working?"}
        time.sleep(5)
        post_events(dummy_data)
        time.sleep(5)

        self.assertTrue(self.server.sock.check_msg)
        terminate_server()

