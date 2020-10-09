from p2prpc.p2p_clientworker import P2PClientworkerApp
from function import p2prpc_analyze_large_file
import os.path as osp

password = "super secret password"
path = osp.join(osp.abspath(osp.dirname(__file__)), 'workerdb')

clientworker_app = P2PClientworkerApp("network_discovery_worker.txt", password=password, cache_path=path)

clientworker_app.register_p2p_func(can_do_work_func=lambda: True)(p2prpc_analyze_large_file)
clientworker_app.run(host='0.0.0.0')
