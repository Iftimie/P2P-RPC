import io
from p2prpc.p2p_client import p2p_progress_hook, p2p_save, p2p_load
import time


def p2prpc_analyze_large_file(video_handle: io.IOBase, arg2: int) -> {"res_var": int}:
    p2p_progress_hook(80, 100)
    precomputed = p2p_load('some_variable')
    if precomputed is None:
        precomputed = 10
        p2p_save('some_variable', precomputed)
        time.sleep(100000)  # asume something bad will happen. this allows time to terminate and restart
    video_handle.close()
    return {"res_var": 10}

