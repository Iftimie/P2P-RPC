from p2prpc.p2p_client import create_p2p_client_app
from function import p2prpc_analyze_large_file
import os.path as osp
from pymongo import MongoClient
import os
import logging
from test_utils import mockupdate, clean_func, query

logger = logging.getLogger(__name__)

client_app = create_p2p_client_app("discovery.txt", password="super secret password", cache_path=osp.join(osp.abspath(osp.dirname(__file__)), 'clientdb'))

p2prpc_analyze_large_file = client_app.register_p2p_func()(p2prpc_analyze_large_file)

res = p2prpc_analyze_large_file(video_handle=open(__file__, 'rb'), arg2=160)
res.upload_job.join()

assert len(query('mongo-client'))==1
assert len(query('mongo-broker'))==1

mockupdate('mongo-broker', {"identifier": res.p2pclientarguments.remote_args_identifier}, {"res_var": 10, 'progress': 100.0})

print(query('mongo-broker'))
print(res.get())

clean_func(('mongo-client',))

kwargs = dict(video_handle=open(__file__, 'rb'), arg2=160)
res = p2prpc_analyze_large_file(**kwargs)
print(res.get())

assert len(query('mongo-client'))==1
assert len(query('mongo-broker'))==1

client_app.background_server.shutdown()


