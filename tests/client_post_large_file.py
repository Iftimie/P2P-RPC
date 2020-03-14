import requests
import queue
import threading
from requests_toolbelt import MultipartEncoder

m = MultipartEncoder(
    fields={'field0': 'value', 'field1': 'value',
            'field2': ('filename', open(__file__, 'rb'), 'text/plain')}
    )
for chunk in m:
    print(chunk)

exit()

class WriteableQueue(queue.Queue):

    def write(self, data):
        # An empty string would be interpreted as EOF by the receiving server
        if data:
            self.put(data)

    def __iter__(self):
        return iter(self.get, None)

    def close(self):
        self.put(None)
# quesize can be limited in case producing is faster then streaming
q = WriteableQueue(100)

def post_request(iterable):
    r = requests.post("http://127.0.0.1:5000/", data=iterable)
    print(r.text)

threading.Thread(target=post_request, args=(q,)).start()


def read_in_chunks(file_object, chunk_size=1024):
    """Lazy function (generator) to read a file piece by piece.
    Default chunk size: 1k."""
    while True:
        data = file_object.read(chunk_size)
        if not data:
            break
        yield data


f = r'/home/achellaris/big_data/torrent/torrents/The.Sopranos.S06.720p.BluRay.DD5.1.x264-DON/The.Sopranos.S06E15.Remember.When.720p.BluRay.DD5.1.x264-DON.mkv'
with open(f, 'rb') as file:
    for chunk in read_in_chunks(file_object=file):
        q.write(chunk)
q.close()