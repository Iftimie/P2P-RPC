
from p2prpc.p2p_client import create_p2p_client_app
from function import p2prpc_analyze_large_file
import os.path as osp
import time
import logging
from test_utils import query
logger = logging.getLogger(__name__)

client_app = create_p2p_client_app("discovery.txt", password="super secret password", cache_path=osp.join(osp.abspath(osp.dirname(__file__)), 'clientdb'))


p2prpc_analyze_large_file = client_app.register_p2p_func()(p2prpc_analyze_large_file)

kwargs = dict()
with open("tmp.txt", 'w') as f:
    pass
res = p2prpc_analyze_large_file(video_handle=open("tmp.txt", 'rb'), arg2=10)
res.upload_job.join()
time.sleep(5)

res.terminate()

res.terminate()
res.delete()
assert len(query('mongo-client'))==0
assert len(query('mongo-broker'))==0
assert len(query('mongo-worker'))==0


client_app.background_server.shutdown()


