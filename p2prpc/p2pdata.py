import requests
from flask import request, jsonify, send_file, make_response
from json import dumps, loads
from werkzeug import secure_filename
from .base import P2PBlueprint
from functools import partial
import logging
import io
import traceback
import os
from typing import Tuple
import time
from functools import wraps
import collections.abc
import zipfile
from passlib.hash import sha256_crypt
from .registry_args import serialize_doc_for_db
from .registry_args import deserialize_doc_from_db
from pymongo import MongoClient


def zip_files(files):
    # TODO fix the temporary archive
    directory = os.path.dirname(list(files.values())[0].name)
    zip_path = os.path.join(directory, "archive.ZIP")
    zf = zipfile.ZipFile(zip_path, "w")
    for k, v in files.items():
        zf.write(v.name, os.path.basename(v.name))
    zf.close()
    return {"archive.ZIP": open(zip_path, 'rb')}


def unzip_files(file, up_dir):
    new_paths = dict()
    zf = zipfile.ZipFile(file, 'r')
    for original_filename in zf.namelist():
        filepath = os.path.join(up_dir, original_filename)
        i = 1
        while os.path.exists(filepath):
            filename, file_extension = os.path.splitext(filepath)
            filepath = filename + "_{}_".format(i) + file_extension
            i += 1
        # FIXME in order to stop it creating directories instead of files
        new_paths[original_filename] = zf.extract(original_filename, filepath)
    zf.close()
    return new_paths


def serialize_doc_for_net(data) -> Tuple[dict, str]:
    """
    Function that will serialize dictionary for sending over the network using the flask framework

    Args:
        data: dictionary containing serializable data

    Returns:
        return tuple containing a dictionary with handle for a single file and json
        the json may contain additional information in order to decode the files after they are received at the
        other end
    """
    files = {os.path.basename(v.name): v for k, v in data.items() if isinstance(v, io.IOBase)}
    files_significance = {os.path.basename(v.name): k for k, v in data.items() if isinstance(v, io.IOBase)}
    new_data = {k: v for k, v in data.items() if not isinstance(v, io.IOBase)}
    new_data["files_significance"] = files_significance
    new_data = serialize_doc_for_db(new_data)
    json_obj = dumps(new_data)
    if len(files) >= 2:
        # the reasong for zipping is explained below
        # the flask framework can receive multiple files
        # the requests package can send multiple files
        # the requests package can download a single file (per transaction)
        # the flask framrwork can send only a single file as download
        files = zip_files(files)
    return files, json_obj


def deserialize_doc_from_net(files, json, up_dir, key_interpreter=None):
    """
    Function should be called from a flask route context. Will deserialize data sent from the network.

    Args:
        files: dictionary containing filenames and the file objects
        json: dictionary to be decoded. In case files is not empty there will be some additional information
            in the json that will help decode the files
        up_dir: in case files dictionary is not empty then the files must be stored somewhere
        key_interpreter: dictionary containing keys and types as values helping to decode the json

    Returns:
        decoded data dictionary
        it may look as {"key": io.IOBase, "key1": "value"}
    """
    data = loads(json)

    listkeys = list(files.keys())
    assert len(listkeys) <= 1  # this assert is explained below
    # the flask framework can receive multiple files
    # the requests package can send multiple files
    # the requests package can download a single file (per transaction)
    # the flask framrwork can send only a single file as download
    if listkeys:
        original_filename = listkeys[0]
        filepath = os.path.join(up_dir, original_filename)
        if "archive.ZIP" in original_filename:
            files[original_filename].save(filepath)
            files = unzip_files(filepath, up_dir)
            for k, v in files.items():
                sign = data['files_significance'][k]
                data[sign] = v
            os.remove(filepath)
        else:
            original_filepath = filepath
            i = 1
            while os.path.exists(filepath):
                filename, file_extension = os.path.splitext(original_filepath)
                filepath = filename + "_{}_".format(i) + file_extension
                i += 1
            files[original_filename].save(filepath)
            sign = data["files_significance"][files[original_filename].name]
            data[sign] = filepath
    del data["files_significance"]

    data = deserialize_doc_from_db(data, key_interpreter)
    return data


def password_required(password):
    def internal_decorator(f):
        @wraps(f)
        def wrap(*args, **kwargs):
            if not sha256_crypt.verify(password, request.headers.get('Authorization')):
                return make_response("Unauthorized", 401)
            return f(*args, **kwargs)
        return wrap
    return internal_decorator


def p2p_route_insert_one(mongod_port, db, col, deserializer=deserialize_doc_from_net):
    """
    Function designed to be decorated with flask.app.route
    The function should be partially applied will all arguments

    The function receives a request json and some files that are part of the same datapoint that can be represented with
    a dictionary.
    The json and files are decoded together to return the actual datapoint that will be inserted to database.
    A datapoint could look as {"key1": str, "key2": io.IOBase}

    Args:
        db_path, db, col are part of tinymongo db
        deserializer: function that deserializes the json and files (files should be saved to some directory known by the deserialized function)
    Returns:
        flask Response
    """
    if request.files:
        filename = list(request.files.keys())[0]
        files = {secure_filename(filename): request.files[filename]}
    else:
        files = dict()
    data_to_insert = deserializer(files, request.form['json'])
    # this will insert. and if the same data exists then it will crash
    update_one(mongod_port, db, col, data_to_insert, data_to_insert, upsert=True)
    return make_response("ok")


def p2p_route_push_update_one(mongod_port, db, col, deserializer=deserialize_doc_from_net):
    """
    Function designed to be decorated with flask.app.route
    The function should be partially applied will all arguments

    Push update refers to the fact that the current node is reachable and new data can be pushed to it.
    The data can create a graph of connections thus in order for the data to be syncronized, the nodes that also have the same data
    will be updated from this call.
    A depth first search will be made and visited nodes will be returned

    When called using http the formular should contain the following keys whose values are actually some jsons
    that can be further decoded to an actual dictionary
    update_json: the json that encodes a dictionary that contains new data
    filter_json: the json that encodes a dictionaty that contains the filter which will be used to search for the datapoint to be updated using tinymongo
    visited_json: nodes that have already been visited
    recursive: whether this function should propagate the update or not to other nodes related to this datapoint

    Args:
        db_path, db, col are part of tinymongo db
        deserializer: function that deserializes the json and files (files should be saved to some directory known by the deserialized function)
    Returns:
        set of visited nodes as a json flask response

    """
    if request.files:
        filename = list(request.files.keys())[0]
        files = {secure_filename(filename): request.files[filename]}
    else:
        files = dict()
    update_data = deserializer(files, request.form['update_json'])
    filter_data = deserializer({}, request.form['filter_json'])
    visited_nodes = loads(request.form['visited_json'])
    recursive = request.form["recursive"]

    if recursive:
        visited_nodes = p2p_push_update_one(mongod_port, db, col, filter_data, update_data, visited_nodes=visited_nodes)
    return jsonify(visited_nodes)


def p2p_route_pull_update_one(mongod_port, db, col, serializer=serialize_doc_for_net):
    """
    Function designed to be decorated with flask.app.route
    The function should be partially applied will all arguments

    Pull update refers to the fact that the current node is reachable and data can be pulled from it. This is useful for
    nodes that are not reachable

    When called using http the formular should contain the following keys whose values are actually some jsons
    that can be further decoded to an actual dictionary
    filter_json: the json that encodes a dictionaty that contains the filter which will be used to search for the datapoint to be updated using tinymongo
    req_keys_json: the required keys to be downloaded
    hint_file_keys_json: some key values are files and these need to be explicityly declared

    Args:
        db_path, db, col are part of tinymongo db
        serializer: function that serializes the datapoint (files should be saved to some directory known by the deserialized function)
    Returns:
        flask response that will contain the dictionary as a json in header metadata and one file (which may be an archive for multiple files)
    """
    req_keys = loads(request.form['req_keys_json'])
    filter_data = loads(request.form['filter_json'])
    hint_file_keys = loads(request.form['hint_file_keys_json'])
    required_list = list(MongoClient(port=mongod_port)[db][col].find(filter_data))
    if not required_list:
        return make_response("filter" + str(filter_data) + "resulted in empty collection")
    # TODO there is a case when filter from client contains both local and remote identifiers, and also both identifiers can be seen here
    #  as 2 different items in collections because both were inserted at different times. In this case error should be returned and the user
    #  should resubmit the work with different arguments (thus different hash)
    #  also, old requests should be deleted in order to minimize the posibility of these events
    assert len(required_list) == 1
    required_data = required_list[0]
    data_to_send = {}
    for k in req_keys:
        if k not in hint_file_keys:
            data_to_send[k] = required_data[k]
        else:
            data_to_send[k] = open(required_data[k], 'rb') if required_data[k] is not None else None

    # TODO in case data is not ready yet, it will crash. fix this so that the log is not polluted with exceptions
    files, json = serializer(data_to_send)
    if files:
        k = list(files.keys())[0]
        result = send_file(list(files.values())[0].name, as_attachment=True)
        result.headers['update_json'] = json
    else:
        result = make_response("")
        # result = send_file(__file__, as_attachment=True)
        result.headers['update_json'] = json
    return result


def create_p2p_blueprint(up_dir, db_url, key_interpreter=None, current_address_func=lambda: None):
    p2p_blueprint = P2PBlueprint("p2p_blueprint", __name__, role="storage")
    new_deserializer = partial(deserialize_doc_from_net, up_dir=up_dir)
    p2p_route_insert_one_func = (wraps(p2p_route_insert_one)(partial(p2p_route_insert_one, db_path=db_url, deserializer=new_deserializer, key_interpreter=key_interpreter)))
    p2p_blueprint.route("/insert_one/<db>/<col>", methods=['POST'])(p2p_route_insert_one_func)
    p2p_route_push_update_one_func = (wraps(p2p_route_push_update_one)(partial(p2p_route_push_update_one, db_path=db_url, deserializer=new_deserializer)))
    p2p_blueprint.route("/push_update_one/<db>/<col>", methods=['POST'])(p2p_route_push_update_one_func)
    p2p_route_pull_update_one_func = (wraps(p2p_route_pull_update_one)(partial(p2p_route_pull_update_one, db_path=db_url)))
    p2p_blueprint.route("/pull_update_one/<db>/<col>", methods=['POST'])(p2p_route_pull_update_one_func)
    return p2p_blueprint


def validate_document(document):
    for k in document:
        if isinstance(document[k], dict):
            raise ValueError("Cannot have nested dictionaries in current implementation")


#https://gitlab.nsd.no/ire/python-webserver-file-submission-poc/blob/master/flask_app.py
def update_one(mongod_port, db, col, query, doc, upsert=False):
    """
    This function is entry point for both insert and update.
    """

    query = serialize_doc_for_db(query)
    doc = serialize_doc_for_db(doc)

    validate_document(doc)
    validate_document(query)

    collection = MongoClient(port=mongod_port)[db][col]
    res = list(collection.find(query))
    doc["timestamp"] = time.time()
    if len(res) == 0 and upsert is True:
        if "nodes" not in doc:
            doc["nodes"] = []
        collection.insert(doc)
    elif len(res) == 1:
        collection.update_one(query, {"$set": doc})
    else:
        raise ValueError("Unable to update. Query: {}. Documents: {}".format(str(query), str(res)))


def find(mongod_port, db, col, query, key_interpreter_dict=None):

    query = serialize_doc_for_db(query)

    collection = list(MongoClient(port=mongod_port)[db][col].find(query))
    for i in range(len(collection)):
        collection[i] = deserialize_doc_from_db(collection[i], key_interpreter_dict)
        # TODO should delete keys such as nodes, _id, timestamp?

    return collection


def p2p_insert_one(db_path, db, col, document, nodes, serializer=serialize_doc_for_net, current_address_func=lambda : None, do_upload=True,
                   password=""):
    """
    post_func is used especially for testing
    current_address_func: self_is_reachable should be called or actually a function that returns the current address
    """
    logger = logging.getLogger(__name__)

    current_addr = current_address_func()
    data = {k:v for k, v in document.items()}
    try:
        update = data
        update["nodes"] = nodes
        update["current_address"] = current_addr
        # TODO should be insert and throw error if identifier exists
        update_one(db_path, db, col, data, update, upsert=True)
    except ValueError as e:
        logger.info(traceback.format_exc())
        raise e

    if do_upload:
        for i, node in enumerate(nodes):
            # the data sent to a node will not contain in "nodes" list the pointer to that node. only to other nodes
            data["nodes"] = nodes[:i] + nodes[i+1:]
            data["nodes"] += current_addr
            file, json = serializer(data)
            try:
                requests.post("http://{}/insert_one/{}/{}".format(node, db, col), files=file, data={"json": json},
                              headers={'Authorization': password})
            except:
                traceback.print_exc()
                logger.info(traceback.format_exc())
                logger.warning("Unable to post p2p data",)


def p2p_push_update_one(mongod_port, db, col, filter, update,  serializer=serialize_doc_for_net, visited_nodes=None, recursive=True,
                        password=""):
    logger = logging.getLogger(__name__)

    if visited_nodes is None:
        visited_nodes = []
    try:
        update_one(mongod_port, db, col, filter, update, upsert=False)
    except ValueError as e:
        logger.info(traceback.format_exc())
        raise e

    collection = MongoClient(port=mongod_port)[db][col]
    res = list(collection.find(filter))
    if len(res) != 1:
        raise ValueError("Unable to update. Query: {}. Documents: {}".format(str(filter), str(res)))

    nodes = res[0]["nodes"]
    current_node = res[0]["current_address"]
    visited_nodes.append(current_node)

    for i, node in enumerate(nodes):
        if node in visited_nodes:
            continue

        files, update_json = serializer(update)
        _, filter_json = serializer(filter)
        visited_json = dumps(visited_nodes)

        try:
            url = "http://{}/push_update_one/{}/{}".format(node, db, col)
            data_to_send = {"update_json": update_json, "filter_json": filter_json, "visited_json": visited_json,
                            "recursive": recursive}
            res = requests.post(url, files=files, data=data_to_send, headers={"Authorization": password})
            if res.status_code == 200:
                if isinstance(res.json, collections.Callable): # from requests lib
                    returned_json = res.json()
                else: # is property
                    returned_json = res.json # from Flask test client
                visited_nodes = list(set(visited_nodes) | set(returned_json))
        except:
            logger.info("Unable to post p2p data to node: {}".format(node))

    return visited_nodes


class WrapperSave:
    def __init__(self, response, name):
        self.name = name
        if hasattr(response, "data"): # response from Flask client test
            self.bytes = response.data
        elif hasattr(response, "content"): # response from requests lib
            self.bytes = response.content
        else:
            raise ValueError("response does not have content data")

    def save(self, filepath):
        with open(filepath, 'wb') as f:
            f.write(self.bytes)


def merge_downloaded_data(original_data, merging_data):
    if not merging_data:
        return {}
    update_keys = merging_data[0].keys()
    result_update = {}
    for k in update_keys:
        if "timestamp" in k:continue
        if all(d[k] == original_data[k] for d in merging_data):
            result_update[k] = max(merging_data, key=lambda d: d['timestamp'])[k]
        else:
            result_update[k] = max(merging_data, key=lambda d: d['timestamp'] if d[k] != original_data[k] else 0)[k]

    return result_update


def p2p_pull_update_one(mongod_port, db, col, filter, req_keys, deserializer, hint_file_keys=None, merging_func=merge_downloaded_data,
                        password=""):
    logger = logging.getLogger(__name__)

    if hint_file_keys is None:
        hint_file_keys = []

    req_keys = list(set(req_keys) | set(["timestamp"]))

    collection = MongoClient(port=mongod_port)[db][col]
    collection_res = list(collection.find(filter))
    if len(collection_res) != 1:
        raise ValueError("Unable to update. Query: {}. Documents: {}".format(str(filter), str(collection_res)))

    nodes = collection_res[0]["nodes"]

    files_to_remove_after_download = []
    merging_data = []

    for i, node in enumerate(nodes):

        req_keys_json = dumps(req_keys)
        filter_json = dumps(filter)
        hint_file_keys_json = dumps(hint_file_keys)


        try:
            data_to_send = {"req_keys_json": req_keys_json, "filter_json": filter_json, "hint_file_keys_json": hint_file_keys_json}
            url = "http://{}/pull_update_one/{}/{}".format(node, db, col)
            res = requests.post(url, files={}, data=data_to_send, headers={"Authorization": password})

            if res.status_code == 200:
                update_json = res.headers['update_json']
                files = {}
                if 'Content-Disposition' in res.headers:
                    filename = res.headers['Content-Disposition'].split("filename=")[1]
                    files = {filename: WrapperSave(res, filename)}
                downloaded_data = deserializer(files, update_json)
                merging_data.append(downloaded_data)
            else:
                logger.info(res.content)
        except:
            traceback.print_exc()
            logger.info(traceback.format_exc())
            logger.info("Unable to post p2p data")

    update = merging_func(collection_res[0], merging_data)

    try:
        update_one(mongod_port, db, col, filter, update, upsert=False)
    except ValueError as e:
        logger.info(traceback.format_exc())
        raise e
