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
            print(ret.text + ' ' + str(ret.status_code))

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

    def test_load(self):
        if self.server_up:
            df = open(self.dir + "demo_trip26.txt", 'r')
            data = df.read()
            df.close()
            json_data = json.loads(data)
            for obj in json_data:
                obj['stream_token'] = #TODO
            ret = requests.post(self.url + '/api/load', data={'data': data})
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
