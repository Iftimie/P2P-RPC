from p2prpc.p2p_brokerworker import P2PBrokerworkerApp
from function import analyze_large_file
import os.path as osp

password = "super secret password"
path = osp.join(osp.dirname(__file__), 'brokerworkerdb')
broker_worker_app = P2PBrokerworkerApp(None, password=password, cache_path=path)

broker_worker_app.register_p2p_func(can_do_locally_func=lambda: True)(analyze_large_file)
broker_worker_app.start_background_threads()
broker_worker_app.run(host='0.0.0.0')
