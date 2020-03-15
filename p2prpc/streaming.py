from requests_toolbelt.streaming_iterator import StreamingIterator
import os
import requests
import pickle
import struct
import tempfile
import time
import shutil

CHUNK_SIZE = 2**13


class DataFilesStreamer:
    def __init__(self, files, data):
        self.encoded_data = pickle.dumps(data)
        self.files = files

    def __len__(self):
        total = 0
        total += len(struct.pack('!i', len(self.encoded_data)))
        total += len(self.encoded_data)
        total += len(struct.pack('!i', len(self.files)))
        for filename, fobj in self.files.items():
            bfilename = filename.encode()
            total += len(struct.pack('!i', len(bfilename)))
            total += len(bfilename)
            total += len(struct.pack('!Q', len(fobj.name)))
            total += os.path.getsize(fobj.name)
        return total

    def __iter__(self):
        yield struct.pack('!i', len(self.encoded_data))
        yield self.encoded_data
        yield struct.pack('!i', len(self.files))
        for filename, fobj in self.files.items():
            bfilename = filename.encode()
            yield struct.pack('!i', len(bfilename))
            yield bfilename
            yield struct.pack('!Q', os.path.getsize(fobj.name))

            count = 0
            while True:
                count += 1
                data = fobj.read(CHUNK_SIZE)
                # TODO this will still fill up memory, but much less. a sleep is a trivial solution
                if not data:
                    break
                yield data


class WrapperReceivedFile:

    def __init__(self, filesize, stream, name):
        self.name = name
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
            filesize = struct.unpack('!Q', self.stream.read(8))[0]
            self.files[filename] = WrapperReceivedFile(filesize, stream, name=filename)

