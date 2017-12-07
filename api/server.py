""" Flask-API server for communications b/w CLI and services. """
import json
from flask import Flask, request
from flask_restful import reqparse
from strom.dstream.dstream import DStream
from strom.coordinator.coordinator import Coordinator
from strom.kafka.producer.producer import Producer
from strom.utils.logger.logger import logger

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
class Server():
    def __init__(self):
        self.expected_args = [
        'template', 'data', 'source', 'topic', 'token', 'stream_data', 'stream_template'
        ]
        self.parser = reqparse.RequestParser()
        self.coordinator = Coordinator()
        self.kafka_url = '127.0.0.1:9092'
        self.load_producer = Producer(self.kafka_url, b'load')
        self.dstream = None
        for word in self.expected_args:
            self.parser.add_argument(word)

    def _dstream_new(self):
        self.dstream = DStream() # NOTE TODO

    def parse(self):
        ret = self.parser.parse_args()
        return ret
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

app = Flask(__name__.split('.')[0])
srv = Server()

def define():
    """ Collect template for DStream init and return stream_token. """
    args = srv.parse()
    template = args['template'] #   dstream template
    srv._dstream_new()
    try:
        json_template = json.loads(template)
        logger.debug("define: json.loads done")
        srv.dstream.load_from_json(json_template)
        logger.debug("define: dstream.load_from_json done")
        srv.coordinator.process_template(srv.dstream)
        logger.debug("define: coordinator.process-template done")
    except Exception as ex:
        logger.warn("Server Error in define: Template loading/processing - {}".format(ex))
        return '{}'.format(ex), 400
    else:
        return str(srv.dstream['stream_token']), 200

def add_source(): #NOTE TODO
    """ Collect data source and set in DStream field """
    args = srv.parse()
    if args['topic'] is not None:
        topic = args['topic']   #   kafka topic
        print(topic)
    source = args['source'] #   file/kafka
    token = args['token']   #   stream_token
    print(source)
    print(token)
    return 'Success.', 200

def load():
    """ Collect tokenized data. """
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
        logger.warn("Server Error in load: Data loading/processing - {}".format(ex))
        return '{}'.format(ex), 400
    else:
        return 'Success.', 202

def load_kafka():
    """ Collect data and produce to kafka topic. """
    args = srv.parse()
    try:
        data = args['stream_data'].encode()
        logger.debug("load_kafka: encode stream_data done")
        srv.load_producer.produce(data)
        logger.debug("load_kafka: producer.produce done")
    except Exception as ex:
        logger.fatal("Server Error in kafka_load: Encoding/producing data - {}".format(ex))
        return '{}'.format(ex), 400
    else:
        return 'Success.', 202

def index():
    return 'STROM-API is UP', 200

def get(this):
    """ Returns data, specified by endpoint & URL params. """
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
            result = srv.coordinator.get_events(token)
            logger.debug("get: coordinator.get_events done")
            return ("\n" + str(result) + "\n"), 200
        else:
            return '', 403

# POST
app.add_url_rule('/api/define', 'define', define, methods=['POST'])
app.add_url_rule('/api/add-source', 'add_source', add_source, methods=['POST'])
app.add_url_rule('/api/load', 'load', load, methods=['POST'])
# KAFKA POST
app.add_url_rule('/kafka/load', 'load_kafka', load_kafka, methods=['POST'])
# GET
app.add_url_rule('/', 'index', index, methods=['GET'])
app.add_url_rule('/api/get/<this>', 'get', get, methods=['GET'])

def start():
    """ Entry-point """
    app.run()
if __name__ == '__main__':
    start()
