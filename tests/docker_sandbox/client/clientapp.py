
from p2prpc.p2p_client import create_p2p_client_app
from function import p2prpc_analyze_large_file
import os.path as osp
import time
import logging
logger = logging.getLogger(__name__)

password = "super secret password"
path = osp.join(osp.abspath(osp.dirname(__file__)), 'clientdb')

client_app = create_p2p_client_app("discovery.txt", password=password, cache_path=path)

p2prpc_analyze_large_file = client_app.register_p2p_func()(p2prpc_analyze_large_file)

kwargs = dict(video_handle=open(__file__, 'rb'), arg2=160)
res = p2prpc_analyze_large_file(**kwargs)
print(res.get())

client_app.background_server.shutdown()


