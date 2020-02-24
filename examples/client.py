from p2prpc.p2p_client import create_p2p_client_app
from .function import analyze_large_file

password = "super secret password"
path = 'clientdb'

client_app = create_p2p_client_app("network_discovery_client.txt", password=password, cache_path=path)

analyze_large_file = client_app.register_p2p_func(can_do_locally_func=lambda: False)(analyze_large_file)

res = analyze_large_file(video_handle=open(__file__, 'rb'), arg2=100)
print(res.get())
res = analyze_large_file(video_handle=open(__file__, 'rb'), arg2=200)
print(res.get())

client_app.background_server.shutdown()
