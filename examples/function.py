import io


def analyze_large_file(video_handle: io.IOBase, arg2: int) -> {"results_file1": io.IOBase,
                                                               "results_file2": io.IOBase,
                                                               "res_var": int}:
    video_handle.close()
    return {"results_file1": open(video_handle.name, 'rb'),
            "results_file2": open(__file__, 'rb'),
            "res_var": 10}
