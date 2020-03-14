from flask import Flask, request
app = Flask(__name__)
import struct
import pickle
import tempfile
import time
import shutil
import os
CHUNK_SIZE = 2**16


class WrapperReceivedFile:

    def __init__(self, filesize, stream):
        fo, path = tempfile.mkstemp()

        while filesize > 0:
            data = stream.read(min(filesize, CHUNK_SIZE))
            os.write(fo, data)
            filesize -= len(data)
        os.close(fo)
        self.path = path

    def save(self, filepath):
        shutil.move(self.path, filepath)


class DataFilesReceiver:
    def __init__(self, stream):
        self.stream = stream

        encoded_data_size = self.stream.read(4)
        encoded_data_size = struct.unpack('!i', encoded_data_size)[0]
        encoded_data = self.stream.read(encoded_data_size)
        self.form = pickle.loads(encoded_data)
        self.files = dict()

        num_files = struct.unpack('!i', self.stream.read(4))[0]
        for i in range(num_files):
            filename_size = struct.unpack('!i', self.stream.read(4))[0]
            filename = self.stream.read(filename_size).decode()
            filesize = struct.unpack('!i', self.stream.read(4))[0]
            self.files[filename] = WrapperReceivedFile(filesize, stream)



@app.route("/", methods=["POST"])
def hello():

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
    """

    r = DataFilesReceiver(request.stream)

    print("gets hereeeee")

    print(r.files)
    print(r.form)
    list(r.files.values())[0].save("here.blabla")
    #
    # with open('posted.dat', 'wb') as f:
    #     data = request.stream.read(1024)
    #     while data:
    #         print(data)
    #         f.write(data)
    #         data = request.stream.read(1024)
    #     return "Success!"
    return "ok"


if __name__ == '__main__':
    app.run()