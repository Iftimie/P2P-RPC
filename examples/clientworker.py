from p2prpc.p2p_clientworker import P2PClientworkerApp
from function import analyze_large_file
import os.path as osp

password = "super secret password"
path = osp.join(osp.dirname(__file__), 'clientworkerdb')

clientworker_app = P2PClientworkerApp("network_discovery_clientworker.txt", password=password, cache_path=path)

clientworker_app.register_p2p_func(can_do_work_func=lambda: True)(analyze_large_file)
clientworker_app.run(host='0.0.0.0')
