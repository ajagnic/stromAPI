import unittest
import requests
import time
from threading import Thread
from .server import app, socketio
from strom.utils.configer import configer as config

class TestServer(unittest.TestCase):
    def setUp(self):
        self.dir = "demo_data/"
        self.url = "http://127.0.0.1:5000"

    class ServerThread(Thread):
        def __init__(self):
            super().__init__()
            self.app = app
            self.sock = socketio

        def run(self):
            self.sock.run(self.app)

    server = ServerThread()


    # def test_define(self):
    #     if self.server_up:
    #         tf = open(self.dir + "demo_template.txt", 'r')
    #         template = tf.read()
    #         tf.close()
    #         ret = requests.post(self.url + '/api/define', data={'template': template})
    #         self.assertEqual(ret.status_code, 200)
    #     else:
    #         self.fail("!! SERVER NOT FOUND !!")
    #
    # def test_define_format_fail(self):
    #     if self.server_up:
    #         ef = open(self.dir + "empty.txt", 'r')
    #         empty = ef.read()
    #         ef.close()
    #         ret = requests.post(self.url + '/api/define', data={'template': empty})
    #         self.assertEqual(ret.status_code, 400)
    #     else:
    #         self.fail("!! SERVER NOT FOUND !!")
    #
    # def test_define_get_fail(self):
    #     if self.server_up:
    #         ret = requests.get(self.url + '/api/define')
    #         self.assertEqual(ret.status_code, 405)
    #     else:
    #         self.fail("!! SERVER NOT FOUND !!")
    #
    # def test_load(self):
    #     if self.server_up:
    #         df = open(self.dir + "demo_trip26.txt", 'r')
    #         data = df.read()
    #         tf = open(self.dir + "demo_template.txt", 'r')
    #         tmpl = tf.read()
    #         ret = requests.post(self.url + '/api/define', data={'template': tmpl})
    #         json_data = json.loads(data)
    #         for obj in json_data:
    #             obj['stream_token'] = ret.text
    #         json_dump = json.dumps(json_data)
    #         ret = requests.post(self.url + '/api/load', data={'data': json_dump})
    #         df.close()
    #         tf.close()
    #         self.assertEqual(ret.status_code, 202)
    #     else:
    #         self.fail("!! SERVER NOT FOUND !!")
    #
    # def test_load_format_fail(self):
    #     if self.server_up:
    #         edf = open(self.dir + "empty.txt", 'r')
    #         empty = edf.read()
    #         edf.close()
    #         ret = requests.post(self.url + '/api/load', data={'data': empty})
    #         self.assertEqual(ret.status_code, 400)
    #     else:
    #         self.fail("!! SERVER NOT FOUND !!")
    #
    # def test_load_get_fail(self):
    #     if self.server_up:
    #         ret = requests.get(self.url + '/api/load')
    #         self.assertEqual(ret.status_code, 405)
    #     else:
    #         self.fail("!! SERVER NOT FOUND !!")
    #
    # def test_load_kafka(self):
    #     if self.server_up:
    #         msg = "Hello !%&^!"
    #         data = msg.encode()
    #         ret = requests.post(self.url + '/kafka/load', data={'stream_data': data})
    #         self.assertEqual(ret.status_code, 202)
    #     else:
    #         self.fail("!! SERVER NOT FOUND !!")
    #
    # def test_load_kafka_get_fail(self):
    #     if self.server_up:
    #         ret = requests.get(self.url + '/kafka/load')
    #         self.assertEqual(ret.status_code, 405)
    #     else:
    #         self.fail("!! SERVER NOT FOUND !!")
    #
    # def test_get_events_all(self):
    #     if self.server_up:
    #         tf = open(self.dir + "demo_template.txt", 'r')
    #         tmpl = tf.read()
    #         tf.close()
    #         define_r = requests.post(self.url + '/api/define', data={'template': tmpl})
    #         df = open(self.dir + "demo_trip26.txt", 'r')
    #         data = df.read()
    #         df.close()
    #         json_data = json.loads(data)
    #         for obj in json_data:
    #             obj['stream_token'] = define_r.text
    #         json_dump = json.dumps(json_data)
    #         load_r = requests.post(self.url + '/api/load', data={'data': json_dump})
    #         get_r = requests.get(self.url + '/api/get/events?range=ALL&token=' + define_r.text)
    #         self.assertEqual(get_r.status_code, 200)
    #         self.assertGreater(len(get_r.text), 5)
    #     else:
    #         self.fail("!! SERVER NOT FOUND !!")

    def test_handle_event_detection(self):
        def post_events(events):
           endpoint = 'http://{}:{}/new_event'.format(config['server_host'], config['server_port'])
           r = requests.post(endpoint, json=events)
           return r


        # bstream = json.load(open(self.dir + "events.json"))
        # process_data_sync will eventually run the code below, but for now,
        # we have to call it ourselves
        # tf = open(self.dir + "events.json")
        # tmpl = tf.read()
        # tf.close()

        self.server.start()

        dummy_data = {"key": "why is this not working?"}
        #dumped = json.dumps(dummy_data)
        time.sleep(5)
        post_events(dummy_data)
        time.sleep(5)


        #self.assertEqual(get_r, dumped)

        # get events from the /events endpoint
        #print(get_r)

        # Test from the server side that the socket responds
        self.assertTrue(self.server.sock.check_msg)
