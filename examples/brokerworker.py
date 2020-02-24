from p2prpc.p2p_brokerworker import P2PBrokerworkerApp
from .function import analyze_large_file

password = "super secret password"
path = 'brokerworkerdb'
broker_worker_app = P2PBrokerworkerApp("network_discovery_brokerworker.txt", password=password, cache_path=path)

broker_worker_app.register_p2p_func(can_do_locally_func=False)(analyze_large_file)

broker_worker_app.run(host='0.0.0.0')
