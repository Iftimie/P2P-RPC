from p2prpc.p2p_client import create_p2p_client_app, ServerThread
from p2prpc.p2p_brokerworker import P2PBrokerworkerApp
import os
import io
from shutil import rmtree
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
from p2prpc.p2p_clientworker import P2PClientworkerApp
from p2prpc.p2p_client import select_lru_worker
import pprint


def large_file_function(video_handle: io.IOBase, random_arg: int) -> {"results": io.IOBase}:
    video_handle.close()
    return {"results": open(video_handle.name, 'rb')}


def large_file_function_wait(video_handle: io.IOBase, random_arg: int) -> {"results": io.IOBase}:
    video_handle.close()
    time.sleep(10)
    return {"results": open(video_handle.name, 'rb')}


def actual_large_file_function_wait(video_handle: io.IOBase, random_arg: int) -> {"results": io.IOBase}:
    video_handle.close()
    time.sleep(10)
    return {"results": open(video_handle.name, 'rb')}


def multiple_client_calls(tmpdir):

    ndclient_path = os.path.join(tmpdir, "ndclient.txt")
    ndcw_path = os.path.join(tmpdir, "ndcw.txt")
    cache_client_dir = os.path.join(tmpdir, "client")
    cache_bw_dir = os.path.join(tmpdir, "bw")
    with open(ndclient_path, "w") as f: f.write("localhost:5001")
    with open(ndcw_path, "w") as f: f.write("localhost:5001")
    client_app = create_p2p_client_app(ndclient_path, local_port=5000, mongod_port=5100, cache_path=cache_client_dir)
    client_large_file_function = client_app.register_p2p_func(can_do_locally_func=lambda :False)(large_file_function)
    broker_worker_app = P2PBrokerworkerApp(None, local_port=5001, mongod_port=5101, cache_path=cache_bw_dir)
    broker_worker_app.register_p2p_func(can_do_locally_func=lambda :True)(large_file_function)
    broker_worker_thread = ServerThread(broker_worker_app)
    broker_worker_thread.start()

    with ThreadPoolExecutor(max_workers=10) as executor:
        list_futures_of_futures = []
        for i in range(20):
            future = executor.submit(client_large_file_function, video_handle=open(__file__, 'rb'), random_arg=i)
            list_futures_of_futures.append(future)
        list_futures = [f.result() for f in list_futures_of_futures]
        assert len(list_futures) == 20
        list_results = [f.get() for f in list_futures]
        assert len(list_results) == 20 and all(isinstance(r, dict) for r in list_results)

        list_futures_of_futures = []
        for i in range(20):
            future = executor.submit(client_large_file_function, video_handle=open(__file__, 'rb'), random_arg=i)
            list_futures_of_futures.append(future)
        list_futures = [f.result() for f in list_futures_of_futures]
        assert len(list_futures) == 20
        list_results = [f.get() for f in list_futures]
        assert len(list_results) == 20 and all(isinstance(r, dict) for r in list_results)

        print(os.listdir(cache_bw_dir + "/p2p/large_file_function"))

    client_app.background_server.shutdown()
    broker_worker_thread.shutdown()


def multiple_client_calls_client_worker(tmpdir, port_offset, func, file=None):
    if file is None:
        file = __file__
    client_port = 5000 +port_offset
    broker_port = 5004 +port_offset
    client_worker_port = 5005 +port_offset

    ndclient_path = os.path.join(tmpdir, "ndclient.txt")
    ndcw_path = os.path.join(tmpdir, "ndcw.txt")
    cache_client_dir = os.path.join(tmpdir, "client")
    cache_bw_dir = os.path.join(tmpdir, "bw")
    cache_cw_dir = os.path.join(tmpdir, "cw")
    with open(ndclient_path, "w") as f: f.write("localhost:{}\n".format(broker_port))
    with open(ndcw_path, "w") as f: f.write("localhost:{}\n".format(broker_port))
    client_app = create_p2p_client_app(ndclient_path, local_port=client_port, mongod_port=client_port+100, cache_path=cache_client_dir)
    client_func = client_app.register_p2p_func(can_do_locally_func=lambda: False)(func)

    broker_worker_app = P2PBrokerworkerApp(None, local_port=broker_port, mongod_port=broker_port+100, cache_path=cache_bw_dir)
    broker_worker_app.register_p2p_func(can_do_locally_func=lambda :False)(func)
    broker_worker_thread = ServerThread(broker_worker_app)
    broker_worker_thread.start()
    clientworker_app = P2PClientworkerApp(ndcw_path, local_port=client_worker_port, mongod_port=client_worker_port+100, cache_path=cache_cw_dir)
    clientworker_app.register_p2p_func(can_do_work_func=lambda :True)(func)
    clientworker_thread = ServerThread(clientworker_app)
    clientworker_thread.start()
    while select_lru_worker(client_port) == (None, None):
        time.sleep(3)
        print("Waiting for client to know about broker")
    while select_lru_worker(client_worker_port) == (None, None):
        time.sleep(3)
        print("Waiting for clientworker to know about broker")

    with ThreadPoolExecutor(max_workers=10) as executor:
        num_calls = 1
        list_futures_of_futures = []
        for i in range(num_calls):
            future = executor.submit(client_func, video_handle=open(file, 'rb'), random_arg=i)
            list_futures_of_futures.append(future)
        list_futures = [f.result() for f in list_futures_of_futures]
        assert len(list_futures) == num_calls
        list_results = [f.get() for f in list_futures]
        assert len(list_results) == num_calls and all(isinstance(r, dict) for r in list_results)

        num_calls = 1
        list_futures_of_futures = []
        for i in range(num_calls):
            future = executor.submit(client_func, video_handle=open(file, 'rb'), random_arg=i)
            list_futures_of_futures.append(future)
        list_futures = [f.result() for f in list_futures_of_futures]
        assert len(list_futures) == num_calls
        list_results = [f.get() for f in list_futures]
        assert len(list_results) == num_calls and all(isinstance(r, dict) for r in list_results)

    client_app.background_server.shutdown()
    print("Shutdown client")
    broker_worker_thread.shutdown()
    print("Shutdown brokerworker")
    clientworker_thread.shutdown()
    print("Shutdown clientworker")
    time.sleep(3)


def delete_old_requests(tmpdir, port_offset, func, file=None):
    if file is None:
        file = __file__
    client_port = 5000 + port_offset
    broker_port = 5004 + port_offset
    client_worker_port = 5005 + port_offset

    ndclient_path = os.path.join(tmpdir, "ndclient.txt")
    ndcw_path = os.path.join(tmpdir, "ndcw.txt")
    cache_client_dir = os.path.join(tmpdir, "client")
    cache_bw_dir = os.path.join(tmpdir, "bw")
    cache_cw_dir = os.path.join(tmpdir, "cw")
    with open(ndclient_path, "w") as f: f.write("localhost:{}\n".format(broker_port))
    with open(ndcw_path, "w") as f: f.write("localhost:{}\n".format(broker_port))
    client_app = create_p2p_client_app(ndclient_path, local_port=client_port, mongod_port=client_port+100, cache_path=cache_client_dir)
    client_func = client_app.register_p2p_func(can_do_locally_func=lambda: False)(func)

    broker_worker_app = P2PBrokerworkerApp(None, local_port=broker_port, mongod_port=broker_port+100, cache_path=cache_bw_dir,
                                           old_requests_time_limit=(1/3600) * 30)
    broker_worker_app.register_p2p_func(can_do_locally_func=lambda :False)(func)
    broker_worker_thread = ServerThread(broker_worker_app, processes=10)
    broker_worker_thread.start()
    clientworker_app = P2PClientworkerApp(ndcw_path, local_port=client_worker_port, mongod_port=client_worker_port+100, cache_path=cache_cw_dir)
    clientworker_app.register_p2p_func(can_do_work_func=lambda :True)(func)
    clientworker_thread = ServerThread(clientworker_app)
    clientworker_thread.start()
    while select_lru_worker(client_port) == (None, None):
        time.sleep(3)
        print("Waiting for client to know about broker")
    while select_lru_worker(client_worker_port) == (None, None):
        time.sleep(3)
        print("Waiting for clientworker to know about broker")

    with ThreadPoolExecutor(max_workers=10) as executor:
        num_calls = 2
        list_futures_of_futures = []
        for i in range(num_calls):
            future = executor.submit(client_func, video_handle=open(file, 'rb'), random_arg=i)
            list_futures_of_futures.append(future)
        list_futures = [f.result() for f in list_futures_of_futures]
        assert len(list_futures) <= num_calls
        list_results = [f.get() for f in list_futures]
        assert len(list_results) == num_calls and all(isinstance(r, dict) for r in list_results)

    from pymongo import  MongoClient
    while True:
        col = list(MongoClient(port=broker_port+100)["p2p"][func.__name__].find({}))
        if len(col) != 0:
            print("Waiting to delete the following items")
            pprint.pprint(col)
            time.sleep(3)
        else:
            break

    client_app.background_server.shutdown()
    print("Shutdown client")
    broker_worker_thread.shutdown()
    print("Shutdown brokerworker")
    clientworker_thread.shutdown()
    print("Shutdown clientworker")
    time.sleep(3)


def clean_and_create():
    test_dir = "/home/achellaris/delete_test_dir"
    if os.path.exists(test_dir):
        rmtree(test_dir)
        while os.path.exists(test_dir):
            time.sleep(3)
    os.mkdir(test_dir)
    return test_dir


def upload_only_no_execution_multiple_large_files(tmpdir, port_offset, func, file):
    client_port = 5000 + port_offset
    broker_port = 5004 + port_offset

    ndclient_path = os.path.join(tmpdir, "ndclient.txt")
    cache_client_dir = os.path.join(tmpdir, "client")
    cache_bw_dir = os.path.join(tmpdir, "bw")
    with open(ndclient_path, "w") as f: f.write("localhost:{}\n".format(broker_port))
    client_app = create_p2p_client_app(ndclient_path, local_port=client_port, mongod_port=client_port+100, cache_path=cache_client_dir)
    client_func = client_app.register_p2p_func(can_do_locally_func=lambda: False)(func)

    broker_worker_app = P2PBrokerworkerApp(None, local_port=broker_port, mongod_port=broker_port+100, cache_path=cache_bw_dir,
                                           old_requests_time_limit=(1/3600) * 10, include_finished=False)
    broker_worker_app.register_p2p_func(can_do_locally_func=lambda :False)(func)
    broker_worker_thread = ServerThread(broker_worker_app, 10)
    broker_worker_thread.start()
    while select_lru_worker(client_port) == (None, None):
        time.sleep(3)
        print("Waiting for client to know about broker")

    with ThreadPoolExecutor(max_workers=10) as executor:
        num_calls = 5
        list_futures_of_futures = []
        for i in range(num_calls):
            future = executor.submit(client_func, video_handle=open(file, 'rb'), random_arg=i)
            list_futures_of_futures.append(future)
        list_futures = [f.result() for f in list_futures_of_futures]
        assert len(list_futures) <= num_calls

    from pymongo import MongoClient
    while True:
        col = list(MongoClient(port=broker_port+100)["p2p"][func.__name__].find({}))
        if len(col) != 0:
            print("Waiting to delete the following items")
            pprint.pprint(col)
            time.sleep(3)
        else:
            break

    client_app.background_server.shutdown()
    print("Shutdown client")
    broker_worker_thread.shutdown()
    print("Shutdown brokerworker")
    time.sleep(3)


def crashing_function(video_handle: io.IOBase, random_arg: int) -> {"results": str}:
    video_handle.close()
    return {"results": "okay"}


def function_crash_on_broker_test(tmpdir, port_offset, func, file):
    client_port = 5000 + port_offset
    broker_port = 5004 + port_offset

    ndclient_path = os.path.join(tmpdir, "ndclient.txt")
    cache_client_dir = os.path.join(tmpdir, "client")
    cache_bw_dir = os.path.join(tmpdir, "bw")
    with open(ndclient_path, "w") as f: f.write("localhost:{}\n".format(broker_port))
    client_app = create_p2p_client_app(ndclient_path, local_port=client_port, mongod_port=client_port+100, cache_path=cache_client_dir)
    client_func = client_app.register_p2p_func(can_do_locally_func=lambda: False)(func)

    broker_worker_app = P2PBrokerworkerApp(None, local_port=broker_port, mongod_port=broker_port+100, cache_path=cache_bw_dir)
    broker_worker_app.register_p2p_func(can_do_locally_func=lambda :True)(func)
    broker_worker_thread = ServerThread(broker_worker_app, 10)
    broker_worker_thread.start()
    while select_lru_worker(client_port) == (None, None):
        time.sleep(3)
        print("Waiting for client to know about broker")

    with ThreadPoolExecutor(max_workers=10) as executor:
        num_calls = 10
        list_futures_of_futures = []
        for i in range(num_calls):
            future = executor.submit(client_func, video_handle=open(file, 'rb'), random_arg=i)
            list_futures_of_futures.append(future)
        list_futures = [f.result() for f in list_futures_of_futures]
        assert len(list_futures) <= num_calls
        list_results = [f.get() for f in list_futures]
        assert len(list_results) == num_calls and all(isinstance(r, dict) for r in list_results)
        print(list_results)

    time.sleep(10)
    client_app.background_server.shutdown()
    print("Shutdown client")
    broker_worker_thread.shutdown()
    print("Shutdown brokerworker")
    time.sleep(3)


if __name__ == "__main__":
    # multiple_client_calls(clean_and_create())
    # multiple_client_calls_client_worker(clean_and_create(), 0, func=large_file_function)
    # multiple_client_calls_client_worker(clean_and_create(), 0, func=large_file_function_wait)
    # delete_old_requests(clean_and_create(), 100, func=actual_large_file_function_wait,
    #                     file='/home/achellaris/big_data/torrent/torrents/Wim Hof Method/03 - Breathing/Extended breathing exercise.mp4')
    # largef = r'/home/achellaris/big_data/torrent/torrents/The.Sopranos.S06.720p.BluRay.DD5.1.x264-DON/The.Sopranos.S06E15.Remember.When.720p.BluRay.DD5.1.x264-DON.mkv'
    # upload_only_no_execution_multiple_large_files(clean_and_create(), 1010, func=actual_large_file_function_wait,
    #                     file=largef)
    function_crash_on_broker_test(clean_and_create(), 1410, func=crashing_function,
                        file=__file__)

    clean_and_create()
