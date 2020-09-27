from p2prpc.p2p_client import create_p2p_client_app
from examples.function import analyze_large_file
import os.path as osp
import time
import logging
logger = logging.getLogger(__name__)

password = "super secret password"
path = osp.join(osp.abspath(osp.dirname(__file__)), 'clientdb')

client_app = create_p2p_client_app("network_discovery_client.txt", password=password, cache_path=path)

analyze_large_file = client_app.register_p2p_func()(analyze_large_file)

print(__file__)
# res = analyze_large_file(video_handle=open(__file__, 'rb'), arg2=100)
# print(res.get())
res = analyze_large_file(video_handle=open(__file__, 'rb'), arg2=160)
logger.error("Going to sleep")
time.sleep(5)
logger.error("Going to terminate")
res.terminate()
logger.error("Going to restart")
res.restart()

logger.error("Going to get")
print(res.get())

client_app.background_server.shutdown()
