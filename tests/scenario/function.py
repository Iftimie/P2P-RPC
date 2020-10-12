import io
from p2prpc.p2p_client import p2p_progress_hook
import time


def p2prpc_analyze_large_file(video_handle: io.IOBase, arg2: int) -> {"res_var": int}:
    p2p_progress_hook(80, 100)
    time.sleep(5)
    video_handle.close()
    return {"res_var": 10}

