from requests_toolbelt.streaming_iterator import StreamingIterator
import os
import requests
import pickle
import struct


def some_function():
    f = r'/home/achellaris/big_data/torrent/torrents/The.Sopranos.S06.720p.BluRay.DD5.1.x264-DON/The.Sopranos.S06E15.Remember.When.720p.BluRay.DD5.1.x264-DON.mkv'
    f = __file__
    with open(f, 'rb') as file_object:
        while True:
            data = file_object.read(1024)
            if not data:
                break
            yield data
    some_data = {"some_data": "some_datasdasda"}
    data = pickle.dumps(some_data)
    print(data)
    yield data

def some_function_size():
    f = r'/home/achellaris/big_data/torrent/torrents/The.Sopranos.S06.720p.BluRay.DD5.1.x264-DON/The.Sopranos.S06E15.Remember.When.720p.BluRay.DD5.1.x264-DON.mkv'
    f = __file__

    some_data = {"some_data": "some_datasdasda"}
    data = pickle.dumps(some_data)

    return os.path.getsize(f) + len(data)

generator = some_function()  # Create your generator
size = some_function_size()  # Get your generator's size

largef = r'/home/achellaris/big_data/torrent/torrents/The.Sopranos.S06.720p.BluRay.DD5.1.x264-DON/The.Sopranos.S06E15.Remember.When.720p.BluRay.DD5.1.x264-DON.mkv'

streamer = StreamingIterator(size, generator)
r = requests.post('http://localhost:5000', data=streamer)
# r = requests.post('http://localhost:5000', data={"some_data":"some_dataaaa"}, files={ "file.txt": open(largef, 'rb')})
print(r.status_code)
