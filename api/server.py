""" Flask server for coordination of processes: device registration, ingestion/processing of data, and retrieval of found events. """
import json
import time

from flask import Flask, request, Response, jsonify
from flask_restful import reqparse
from flask_socketio import SocketIO
from strom.coordinator.coordinator import Coordinator
from strom.dstream.dstream import DStream
from strom.kafka.producer.producer import Producer
from strom.utils.logger.logger import logger
from strom.utils.stopwatch import stopwatch as tk

__version__ = '0.1.0'
__author__ = 'Adrian Agnic <adrian@tura.io>'

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
class Server():
    """ Class for storing information, and initialization of needed modules. """
    def __init__(self):
        self.expected_args = [
        'template', 'data', 'source', 'topic', 'token', 'stream_data', 'stream_template'
        ]
        self.parser = reqparse.RequestParser()
        self.coordinator = Coordinator()
        self.kafka_url = '127.0.0.1:9092'
        self.load_producer = Producer(self.kafka_url, b'load')
        # self.producers = {}
        self.dstream = None
        for word in self.expected_args:
            self.parser.add_argument(word)

    def _dstream_new(self):
        tk['Server._dstream_new'].start()
        dstream = DStream()
        tk['Server._dstream_new'].stop()
        return dstream

    def producer_new(self, topic):
        """
        :param topic: Name of topic to produce to
        :type topic: byte string
        """
        tk['Server.producer_new'].start()
        self.producers[topic] = Producer(self.kafka_url, topic.encode())
        tk['Server.producer_new'].stop()

    def parse(self):
        """ Wrapper function for reqparse.parse_args """
        tk['Server.parse'].start()
        ret = self.parser.parse_args()
        tk['Server.parse'].stop()
        return ret
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

app = Flask(__name__.split('.')[0])
socketio = SocketIO(app)
srv = Server()

def define():
    """ Collect template for DStream init and return stream_token.
    Expects 'template' argument containing user-formatted template.
    """
    tk['define'].start()
    args = srv.parse()
    template = args['template'] #   dstream template
    cur_dstream = srv._dstream_new()
    logger.info("stream token is: {}".format(str(cur_dstream['stream_token'])))
    try:
        tk['define : try (template loading/processing)'].start()
        json_template = json.loads(template)
        logger.debug("define: json.loads done")
        cur_dstream.load_from_json(json_template)
        logger.debug("define: dstream.load_from_json done")
        srv.coordinator.process_template(cur_dstream)
        # srv.producer_new(cur_dstream["engine_rules"]["kafka"])
        logger.debug("define: coordinator.process-template done")
        tk['define : try (template loading/processing)'].stop()
    except Exception as ex:
        logger.warning("Server Error in define: Template loading/processing - {}".format(ex))
        # bad_resp = Response(ex, 400)
        # bad_resp.headers['Access-Control-Allow-Origin']='*'
        # return bad_resp
        return '{}'.format(ex), 400
    else:
        resp = Response(str(cur_dstream['stream_token']), 200)
        resp.headers['Access-Control-Allow-Origin']='*'
        tk['define'].stop()
        return resp

def load():
    """ Collect tokenized data.
    Expects 'data' argument containing user dataset to process.
    """
    args = srv.parse()
    data = args['data'] #   data with token
    try:
        json_data = json.loads(data)
        logger.debug("load: json.loads done")
        token = json_data[0]['stream_token']
        logger.debug("load: got token")
        srv.coordinator.process_data_sync(json_data, token)
        logger.debug("load: coordinator.process_data_sync done")
    except Exception as ex:
        logger.warning("Server Error in load: Data loading/processing - {}".format(ex))
        return '{}'.format(ex), 400
    else:
        return 'Success.', 202

def load_kafka():
    """ Collect data and produce to kafka topic.
    Expects 'stream_data' argument containing user dataset to process.
    """
    # logger.fatal("data hit server load")
    start_load = time.time()
    tk['load_kafka'].start()
    args = srv.parse()
    try:
        tk['load_kafka : try (encoding/producing data)'].start()
        data = args['stream_data'].encode()
        logger.debug("load_kafka: encode stream_data done")
        # kafka_topic = args['topic']
        logger.debug("load_kafka: encode topic done")
        srv.load_producer.produce(data)
        # srv.producers[kafka_topic].produce(data)
        logger.debug("load_kafka: producer.produce done")
        tk['load_kafka : try (encoding/producing data)'].stop()
        logger.fatal("Load kafka route took {:.5f} seconds".format(time.time() - start_load))
    except Exception as ex:
        logger.fatal("Server Error in kafka_load: Encoding/producing data - {}".format(ex))
        # bad_resp = Response(ex, 400)
        # bad_resp.headers['Access-Control-Allow-Origin']='*'
        # return bad_resp
        return '{}'.format(ex), 400
    else:
        resp = Response('Success.', 202)
        resp.headers['Access-Control-Allow-Origin']='*'
        tk['load_kafka'].stop()
        return resp


def index():
    resp = Response('STROM-API is UP', 200)
    resp.headers['Access-Control-Allow-Origin']='*'
    return resp

def get(this):
    """ Returns data, specified by endpoint & URL params.
    Expects multiple url arguments: range or time, and token.
    """
    tk['get'].start()
    time_range = request.args.get('range', '')
    time = request.args.get('time', '')
    token = request.args.get('token', '')
    print(this) #   endpoint: raw, filtered, derived_params, events
    print(time)
    print(time_range)
    if time_range:
        logger.debug("get: got time_range")
        if time_range == 'ALL':
            logger.debug("get: time_range is ALL")
            tk['get : coordinator.get_events'].start()
            result = srv.coordinator.get_events(token)
            tk['get : coordinator.get_events'].stop()
            logger.debug("get: coordinator.get_events done")
            tk['get'].stop()
            return ("\n" + str(result) + "\n"), 200
        else:
            return '', 403

def handle_event_detection():
    tk['handle_event_detection'].start()
    json_data = request.get_json()
    if json_data is not None:
        if "event" in json_data:
            if "data" in json_data:
                tk['handle_event_detection : start time'].time()
                tk['handle_event_detection : socketio.emit'].start()
                socketio.emit(json_data["event"], json.dumps(json_data["data"]))
                tk['handle_event_detection : socketio.emit'].stop()
            else:
                raise ValueError('Missing event data field: data')
        else:
            raise ValueError('Missing event name field: event')
    else:
        raise RuntimeError("No event data to return")
    tk['handle_event_detection'].stop()
    return jsonify(json_data)

# def add_source():
#     """ Collect data source and set in DStream field """
#     args = srv.parse()
#     if args['topic'] is not None:
#         topic = args['topic']   #   kafka topic
#         print(topic)
#     source = args['source'] #   file/kafka
#     token = args['token']   #   stream_token
#     print(source)
#     print(token)
#     return 'Success.', 200

# POST
app.add_url_rule('/api/define', 'define', define, methods=['POST'])
# app.add_url_rule('/api/add-source', 'add_source', add_source, methods=['POST'])
app.add_url_rule('/api/load', 'load', load, methods=['POST'])
app.add_url_rule('/new_event', 'handle_event_detection', handle_event_detection, methods=['POST'])
# KAFKA POST
app.add_url_rule('/kafka/load', 'load_kafka', load_kafka, methods=['POST'])
app.add_url_rule('/api/kafka/load', 'load_kafka', load_kafka, methods=['POST'])
# GET
app.add_url_rule('/', 'index', index, methods=['GET'])
app.add_url_rule('/api/get/<this>', 'get', get, methods=['GET'])



def start():
    """ Entry-point """
    socketio.run(app)
if __name__ == '__main__':
    start()
