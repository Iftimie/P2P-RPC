import io
from p2prpc.p2p_client import p2p_progress_hook
import time


def p2prpc_analyze_large_file(video_handle: io.IOBase, arg2: int) -> {"res_var": int}:
    p2p_progress_hook(80, 100)
    for i in range(100000):
        print("asdasdasdasdaasdasdasdasdasdasdasdasdasdasdasdasd" + str(i))
        time.sleep(0.001)
    video_handle.close()
    return {"res_var": 10}

