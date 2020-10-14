
from p2prpc.p2p_client import create_p2p_client_app
from function import p2prpc_analyze_large_file
import os.path as osp
import time
import logging
from test_utils import query
from p2prpc.monitoring import function_call_states
logger = logging.getLogger(__name__)

client_app = create_p2p_client_app("discovery.txt", password="super secret password", cache_path=osp.join(osp.abspath(osp.dirname(__file__)), 'clientdb'))


p2prpc_analyze_large_file = client_app.register_p2p_func()(p2prpc_analyze_large_file)

kwargs = dict()
with open("tmp.txt", 'w') as f:
    pass
res = p2prpc_analyze_large_file(video_handle=open("tmp.txt", 'rb'), arg2=10)
res.upload_job.join()
time.sleep(10)

res.terminate()

print(function_call_states(client_app))
client_app.background_server.shutdown()


