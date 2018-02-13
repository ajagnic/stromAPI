""" Flask server for coordination of processes: device registration, ingestion/processing of data, and retrieval of found events. """
import json
import time

from flask import Flask, request, Response, jsonify
from multiprocessing import Pipe
from flask_restful import reqparse
from flask_socketio import SocketIO
from strom.coordinator.coordinator import Coordinator
from strom.dstream.dstream import DStream
from strom.utils.logger.logger import logger
from strom.utils.stopwatch import stopwatch as tk
from strom.engine.engine import EngineThread

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
        self.dstream = None
        for word in self.expected_args:
            self.parser.add_argument(word)

        # ENGINE
        self.server_conn, self.engine_conn = Pipe()
        self.engine = EngineThread(self.engine_conn, buffer_max_batch=10, buffer_max_seconds=1)
        self.engine.start()# NOTE  POSSIBLE ISSUE WHEN MODIFYING BUFFER PROPS FROM TEST

    def _dstream_new(self):
        tk['Server._dstream_new'].start()
        dstream = DStream()
        tk['Server._dstream_new'].stop()
        return dstream

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
        unjson_data = json.loads(data)
        logger.debug("load: json.loads done")
        token = unjson_data[0]['stream_token']
        logger.debug("load: got token")
        if type(unjson_data) is dict:
            srv.server_conn.send(unjson_data)# NOTE CHECK DATA FORMATS COMPARED TO LOAD_KAFKA
        elif type(unjson_data) is list:
            for d in unjson_data:
                srv.server_conn.send(d)
        logger.debug("load: data piped to engine buffer")
    except Exception as ex:
        logger.warning("Server Error in load: Data loading/processing - {}".format(ex))
        return '{}'.format(ex), 400
    else:
         resp = Response('Success.', 202)
         resp.headers['Access-Control-Allow-Origin']='*'
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

# POST
app.add_url_rule('/api/define', 'define', define, methods=['POST'])
app.add_url_rule('/api/load', 'load', load, methods=['POST'])
app.add_url_rule('/new_event', 'handle_event_detection', handle_event_detection, methods=['POST'])
# GET
app.add_url_rule('/', 'index', index, methods=['GET'])
app.add_url_rule('/api/get/<this>', 'get', get, methods=['GET'])


def start():
    """ Entry-point """
    socketio.run(app)
if __name__ == '__main__':
    start()
