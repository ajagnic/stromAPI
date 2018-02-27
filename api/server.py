""" Flask server for coordination of processes: device registration, ingestion/processing of data, and retrieval of found events. """
import json
import pickle
from multiprocessing import Pipe
from queue import Queue

from flask import Flask, request, Response, jsonify
from flask_restful import reqparse
from flask_socketio import SocketIO

from strom.coordinator.coordinator import Coordinator
from strom.dstream.dstream import DStream
from strom.engine.engine import EngineThread
from strom.storage.storage_worker import StorageWorker, storage_config
from strom.utils.configer import configer as config
from strom.utils.logger.logger import logger
from strom.utils.stopwatch import stopwatch as tk

from strom.storage.sqlite_interface import SqliteInterface

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

        # STORAGE QUEUE, WORKER AND INTERFACE
        self.storage_queue = Queue()
        self.storage_worker = StorageWorker(self.storage_queue, storage_config, config['storage_type'])
        self.storage_worker.start()

        # NOTE TODO MAKE MORE FLEXIBLE?
        self.storage_interface = SqliteInterface(storage_config['local']['args'][0])
        try:
            self.storage_interface.seed_template_table()
        except:
            pass

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
    print("HERWE")
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
        print("ABOUT TO GO")
        template_df = srv.coordinator.process_template(cur_dstream)
        srv.storage_queue.put(('template', template_df))
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
    # with open("demo_data/demo_data_new.txt", "w") as doc:
    #     doc.write(data)
    # doc.close()
    try:
        unjson_data = json.loads(data)
        logger.debug("load: json.loads done")
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
    token = request.args.get("token", "")
    time_range = request.args.get("range", "")# kwargs containing 'start_ts' and 'end_ts'
    if this == "all":
        res = srv.storage_interface.retrieve_data(token, "*")# TODO figure out passing kwargs
        print(res)
        return res.to_json(), 200#TODO better format w/ params?
    else:
        res = srv.storage_interface.retrieve_data(token, this)
        return res.to_json(), 200


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


def data_storage():
    logger.debug("Starting data storage")

    data = request.data

    parsed = pickle.loads(data)

    logger.debug("putting DataFrame in queue")
    # error handling
    srv.storage_queue.put(('bstream', parsed[0], parsed[1]))
    logger.debug("Finished queuing")

    return 'ok'


def template_storage():
    logger.debug("Starting template storage")
    d = request.data
    parsed = pickle.loads(d)
    # error handling
    logger.debug("putting template in queue")
    srv.storage_queue.put('template', parsed)
    logger.debug("Finished queuing")

    return 'ok'

def retrieve_templates(which):
    template_id = request.args.get("template_id", "")
    stream_token = request.args.get("stream_token", "")
    which = str(which).lower()
    if which == "all":
        if template_id:
            return srv.storage_interface.retrieve_template_by_id(template_id)
        elif stream_token:
            return srv.storage_interface.retrieve_all_by_token(stream_token)
        else:
            return srv.storage_interface.retrieve_all_templates()
    elif which == "latest" or which == "current":
        if template_id:
            return srv.storage_interface.retrieve_current_by_id(template_id)
        elif stream_token:
            return srv.storage_interface.retrieve_current_template(stream_token)
    else:
        return "Value Error", 403


# POST
app.add_url_rule('/api/define', 'define', define, methods=['POST'])
app.add_url_rule('/api/load', 'load', load, methods=['POST'])
app.add_url_rule('/new_event', 'handle_event_detection', handle_event_detection, methods=['POST'])
app.add_url_rule('/data_storage', 'data_storage', data_storage, methods=['POST'])
app.add_url_rule('/template_storage', 'template_storage', template_storage, methods=['POST'])

# GET
app.add_url_rule('/', 'index', index, methods=['GET'])
app.add_url_rule('/api/get/<this>', 'get', get, methods=['GET'])
app.add_url_rule('/api/retrieve/<which>', 'retrieve_templates', retrieve_templates, methods=['GET'])


def start():
    """ Entry-point """
    socketio.run(app)
if __name__ == '__main__':
    start()
