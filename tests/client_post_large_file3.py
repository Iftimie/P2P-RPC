from requests_toolbelt.streaming_iterator import StreamingIterator
import os
import requests
import pickle
import struct
import time


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
            total += len(struct.pack('!i', len(fobj.name)))
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
            yield struct.pack('!i', len(fobj.name))
            while True:
                data = fobj.read(2**10)
                # TODO this will still fill up memory, but much less. a sleep is a trivial solution
                if not data:
                    break
                yield data


largef = r'/home/achellaris/big_data/torrent/torrents/The.Sopranos.S06.720p.BluRay.DD5.1.x264-DON/The.Sopranos.S06E15.Remember.When.720p.BluRay.DD5.1.x264-DON.mkv'
generator = DataFilesStreamer({"largefile.mkv": open(largef, 'rb')}, data={"some_data": "data"})
streamer = StreamingIterator(len(generator), iter(generator))
r = requests.post('http://localhost:5000', data=streamer)
print(r.status_code)
