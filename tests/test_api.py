import unittest
import requests
import json

class TestServer(unittest.TestCase):
    def setUp(self):
        self.dir = "demo_data/"
        self.url = "http://127.0.0.1:5000"
        self.server_up = False
        try:
            ret = requests.get(self.url)
        except Exception as ex:
            print(ex)
        else:
            self.server_up = True
            print(ret.text)

    def test_define(self):
        if self.server_up:
            tf = open(self.dir + "demo_template.txt", 'r')
            template = tf.read()
            tf.close()
            ret = requests.post(self.url + '/api/define', data={'template': template})
            self.assertEqual(ret.status_code, 200)
        else:
            self.fail("!! SERVER NOT FOUND !!")

    def test_define_format_fail(self):
        if self.server_up:
            ef = open(self.dir + "empty.txt", 'r')
            empty = ef.read()
            ef.close()
            ret = requests.post(self.url + '/api/define', data={'template': empty})
            self.assertEqual(ret.status_code, 400)
        else:
            self.fail("!! SERVER NOT FOUND !!")

    def test_define_get_fail(self):
        if self.server_up:
            ret = requests.get(self.url + '/api/define')
            self.assertEqual(ret.status_code, 405)
        else:
            self.fail("!! SERVER NOT FOUND !!")

    def test_load(self): # TODO hit define and get actual stream_token back
        if self.server_up:
            df = open(self.dir + "demo_trip26.txt", 'r')
            data = df.read()
            df.close()
            tf = open(self.dir + "demo_template.txt", 'r')
            tmpl = tf.read()
            tf.close()
            ret = requests.post(self.url + '/api/define', data={'template': tmpl})
            json_data = json.loads(data)
            for obj in json_data:
                obj['stream_token'] = ret.text
            json_dump = json.dumps(json_data)
            ret = requests.post(self.url + '/api/load', data={'data': json_dump})
            self.assertEqual(ret.status_code, 202)
        else:
            self.fail("!! SERVER NOT FOUND !!")

    def test_load_format_fail(self):
        if self.server_up:
            edf = open(self.dir + "empty.txt", 'r')
            empty = edf.read()
            edf.close()
            ret = requests.post(self.url + '/api/load', data={'data': empty})
            self.assertEqual(ret.status_code, 400)
        else:
            self.fail("!! SERVER NOT FOUND !!")

    def test_load_get_fail(self):
        if self.server_up:
            ret = requests.get(self.url + '/api/load')
            self.assertEqual(ret.status_code, 405)
        else:
            self.fail("!! SERVER NOT FOUND !!")

    def test_load_kafka(self):
        if self.server_up:
            msg = "Hello !%&^!"
            data = msg.encode()
            ret = requests.post(self.url + '/kafka/load', data={'stream_data': data})
            self.assertEqual(ret.status_code, 202)
        else:
            self.fail("!! SERVER NOT FOUND !!")

    def test_load_kafka_get_fail(self):
        if self.server_up:
            ret = requests.get(self.url + '/kafka/load')
            self.assertEqual(ret.status_code, 405)
        else:
            self.fail("!! SERVER NOT FOUND !!")

    def test_get_events_all(self): # TODO get real events back
        if self.server_up:
            ret = requests.get(self.url + '/api/get/events?range=ALL&token=abc123')
            self.assertEqual(ret.status_code, 200)
        else:
            self.fail("!! SERVER NOT FOUND !!")
