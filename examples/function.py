import io
from p2prpc.p2p_client import p2p_progress_hook
import time


def analyze_large_file(video_handle: io.IOBase, arg2: int) -> {"results_file1": io.IOBase,
                                                               "results_file2": io.IOBase,
                                                               "res_var": int}:
    p2p_progress_hook(20, 100)
    time.sleep(5)
    p2p_progress_hook(50, 100)
    time.sleep(5)
    p2p_progress_hook(70, 100)
    time.sleep(5)
    p2p_progress_hook(75, 100)
    time.sleep(5)
    p2p_progress_hook(80, 100)
    time.sleep(5)
    video_handle.close()
    return {"results_file1": open(video_handle.name, 'rb'),
            "results_file2": open(__file__, 'rb'),
            "res_var": 10}
