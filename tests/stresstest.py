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
from pymongo import MongoClient
import sys


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
    client_func = client_app.register_p2p_func()(func)

    broker_worker_app = P2PBrokerworkerApp(None, local_port=broker_port, mongod_port=broker_port+100, cache_path=cache_bw_dir)
    broker_worker_app.register_p2p_func()(func)
    broker_worker_thread = ServerThread(broker_worker_app)
    broker_worker_thread.start()
    clientworker_app = P2PClientworkerApp(ndcw_path, local_port=client_worker_port, mongod_port=client_worker_port+100, cache_path=cache_cw_dir)
    clientworker_app.register_p2p_func(can_do_work_func=lambda :True)(func)
    clientworker_thread = ServerThread(clientworker_app)
    clientworker_thread.start()
    while select_lru_worker(client_app.registry_functions[func.__name__]) == (None, None):
        time.sleep(3)
        print("Waiting for client to know about broker")
    while select_lru_worker(clientworker_app.registry_functions[func.__name__].p2pfunction) == (None, None):
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

    client_app.background_server.shutdown()
    print("Shutdown client")
    broker_worker_thread.shutdown()
    print("Shutdown brokerworker")
    clientworker_thread.shutdown()
    print("Shutdown clientworker")
    time.sleep(3)


def delete_old_requests(tmpdir, port_offset, func, file=None):
    """
    In this situation. It is ok for check_brokerworker_deletion or check_brokerworker_termination
    to show error messages that originate from the broker. What happens is that on the broker side, the
    arguments expired and were deleted. And the worker is trying to pull arguments that no longer exist.
    """
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
    client_func = client_app.register_p2p_func()(func)

    broker_worker_app = P2PBrokerworkerApp(None, local_port=broker_port, mongod_port=broker_port+100, cache_path=cache_bw_dir,
                                           old_requests_time_limit=(1/3600) * 40)
    broker_worker_app.register_p2p_func()(func)
    broker_worker_thread = ServerThread(broker_worker_app, processes=10)
    broker_worker_thread.start()
    clientworker_app = P2PClientworkerApp(ndcw_path, local_port=client_worker_port, mongod_port=client_worker_port+100, cache_path=cache_cw_dir)
    clientworker_app.register_p2p_func(can_do_work_func=lambda :True)(func)
    clientworker_thread = ServerThread(clientworker_app)
    clientworker_thread.start()
    while select_lru_worker(client_app.registry_functions[func.__name__]) == (None, None):
        time.sleep(3)
        print("Waiting for client to know about broker")
    while select_lru_worker(clientworker_app.registry_functions[func.__name__].p2pfunction) == (None, None):
        time.sleep(3)
        print("Waiting for clientworker to know about broker")

    with ThreadPoolExecutor(max_workers=10) as executor:
        num_calls = 2
        list_futures_of_futures = []
        for i in range(num_calls):
            future = executor.submit(client_func, video_handle=open(file, 'rb'), random_arg=i)
            list_futures_of_futures.append(future)
            time.sleep(2)
        list_futures = [f.result() for f in list_futures_of_futures]
        assert len(list_futures) <= num_calls
        list_results = [f.get() for f in list_futures]
        assert len(list_results) == num_calls and all(isinstance(r, dict) for r in list_results)

    from pymongo import  MongoClient
    while True:
        col = list(MongoClient(port=broker_port+100)["p2p"][func.__name__].find({}))
        if len(col) != 0:
            print("Waiting to delete the following items", col)
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
    time.sleep(3)
    return test_dir


def upload_only_no_execution_multiple_large_files(tmpdir, port_offset, func, file):
    client_port = 5000 + port_offset
    broker_port = 5004 + port_offset

    ndclient_path = os.path.join(tmpdir, "ndclient.txt")
    cache_client_dir = os.path.join(tmpdir, "client")
    cache_bw_dir = os.path.join(tmpdir, "bw")
    with open(ndclient_path, "w") as f: f.write("localhost:{}\n".format(broker_port))
    client_app = create_p2p_client_app(ndclient_path, local_port=client_port, mongod_port=client_port+100, cache_path=cache_client_dir)
    client_func = client_app.register_p2p_func()(func)

    broker_worker_app = P2PBrokerworkerApp(None, local_port=broker_port, mongod_port=broker_port+100, cache_path=cache_bw_dir,
                                           old_requests_time_limit=(1/3600) * 10)
    broker_worker_app.register_p2p_func()(func)
    broker_worker_thread = ServerThread(broker_worker_app, 10)
    broker_worker_thread.start()
    while select_lru_worker(client_app.registry_functions[func.__name__]) == (None, None):
        time.sleep(3)
        print("Waiting for client to know about broker")

    with ThreadPoolExecutor(max_workers=10) as executor:
        num_calls = 5
        list_futures_of_futures = []
        for i in range(num_calls):
            future = executor.submit(client_func, video_handle=open(file, 'rb'), random_arg=i)
            time.sleep(1)
            list_futures_of_futures.append(future)

        list_futures = []
        for f in list_futures_of_futures:
            try:
                list_futures.append(f.result().get())
            except:
                list_futures.append(None)
        print(list_futures) # I expect them to be None because altough the upload did finish, the item was quickly deteled due to expiration
        assert len(list_futures) <= num_calls

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
    raise ValueError("Some error")
    return {"results": "okay"}


def function_crash_on_clientworker_test(tmpdir, port_offset, func, file):
    client_port = 5000 + port_offset
    broker_port = 5004 + port_offset

    ndclient_path = os.path.join(tmpdir, "ndclient.txt")
    cache_client_dir = os.path.join(tmpdir, "client")
    cache_bw_dir = os.path.join(tmpdir, "bw")
    with open(ndclient_path, "w") as f: f.write("localhost:{}\n".format(broker_port))
    client_app = create_p2p_client_app(ndclient_path, local_port=client_port, mongod_port=client_port+100, cache_path=cache_client_dir)
    client_func = client_app.register_p2p_func()(func)

    broker_worker_app = P2PBrokerworkerApp(None, local_port=broker_port, mongod_port=broker_port+100, cache_path=cache_bw_dir)
    broker_worker_app.register_p2p_func()(func)
    broker_worker_thread = ServerThread(broker_worker_app, 10)
    broker_worker_thread.start()
    while select_lru_worker(client_app.registry_functions[func.__name__]) == (None, None):
        time.sleep(3)
        print("Waiting for client to know about broker")

    ndcw_path = os.path.join(tmpdir, "ndcw.txt")
    client_worker_port = 5005 + port_offset
    cache_cw_dir = os.path.join(tmpdir, "cw")
    with open(ndcw_path, "w") as f: f.write("localhost:{}\n".format(broker_port))
    clientworker_app = P2PClientworkerApp(ndcw_path, local_port=client_worker_port,
                                          mongod_port=client_worker_port + 100, cache_path=cache_cw_dir)
    clientworker_app.register_p2p_func(can_do_work_func=lambda: True)(func)
    clientworker_thread = ServerThread(clientworker_app)
    clientworker_thread.start()
    while select_lru_worker(clientworker_app.registry_functions[func.__name__].p2pfunction) == (None, None):
        time.sleep(3)
        print("Waiting for clientworker to know about broker")

    with ThreadPoolExecutor(max_workers=10) as executor:
        num_calls = 1
        list_futures_of_futures = []
        for i in range(num_calls):
            future = executor.submit(client_func, video_handle=open(file, 'rb'), random_arg=i)
            list_futures_of_futures.append(future)
        list_futures = [f.result() for f in list_futures_of_futures]
        assert len(list_futures) <= num_calls
        list_results = []
        count_exceptions = 0
        for f in list_futures:
            try:
                list_results.append(f.get())
            except:
                print("exception happened")
                count_exceptions += 1
        assert count_exceptions == num_calls
        # assert len(list_results) == num_calls and all(isinstance(r, dict) for r in list_results)
        # print(list_results)

    client_app.background_server.shutdown()
    print("Shutdown client")
    broker_worker_thread.shutdown()
    print("Shutdown brokerworker")
    clientworker_thread.shutdown()
    print("Shutdown clientworker")
    time.sleep(3)


def long_function_upload(file_handle: io.IOBase) -> {"results": str}:
    time.sleep(10)
    return {"results": "ok"}


def function_restart_unfinished_upload_on_broker(tmpdir, port_offset, func):
    file = r'/home/achellaris/big_data/torrent/torrents/The.Sopranos.S06.720p.BluRay.DD5.1.x264-DON/The.Sopranos.S06E15.Remember.When.720p.BluRay.DD5.1.x264-DON.mkv'

    client_port = 5000 + port_offset
    broker_port = 5004 + port_offset

    ndclient_path = os.path.join(tmpdir, "ndclient.txt")
    cache_client_dir = os.path.join(tmpdir, "client")
    cache_bw_dir = os.path.join(tmpdir, "bw")
    with open(ndclient_path, "w") as f: f.write("localhost:{}\n".format(broker_port))
    client_app = create_p2p_client_app(ndclient_path, local_port=client_port, mongod_port=client_port+100, cache_path=cache_client_dir)
    client_func = client_app.register_p2p_func()(func)

    broker_worker_app = P2PBrokerworkerApp(None, local_port=broker_port, mongod_port=broker_port+100, cache_path=cache_bw_dir)
    broker_worker_app.register_p2p_func()(func)
    broker_worker_thread = ServerThread(broker_worker_app, 10)
    broker_worker_thread.start()
    while select_lru_worker(client_app.registry_functions[func.__name__]) == (None, None):
        time.sleep(3)
        print("Waiting for client to know about broker")

    ndcw_path = os.path.join(tmpdir, "ndcw.txt")
    client_worker_port = 5005 + port_offset
    cache_cw_dir = os.path.join(tmpdir, "cw")
    with open(ndcw_path, "w") as f: f.write("localhost:{}\n".format(broker_port))
    clientworker_app = P2PClientworkerApp(ndcw_path, local_port=client_worker_port,
                                          mongod_port=client_worker_port + 100, cache_path=cache_cw_dir)
    clientworker_app.register_p2p_func(can_do_work_func=lambda: True)(func)
    clientworker_thread = ServerThread(clientworker_app)
    clientworker_thread.start()
    while select_lru_worker(clientworker_app.registry_functions[func.__name__].p2pfunction) == (None, None):
        time.sleep(3)
        print("Waiting for clientworker to know about broker")

    with ThreadPoolExecutor(max_workers=10) as executor:
        pool_future = executor.submit(client_func, file_handle=open(file, 'rb'))
        p2p_future = pool_future.result()
        try:
            p2p_future.restart()
        except Exception as e:
            print(str(e))
            assert "Filter not found" in str(e)
        # assert len(list_results) == num_calls and all(isinstance(r, dict) for r in list_results)
        # print(list_results)
    client_app.background_server.shutdown()
    print("Shutdown client")
    broker_worker_thread.shutdown()
    print("Shutdown brokerworker")
    clientworker_thread.shutdown()
    print("Shutdown clientworker")
    time.sleep(3)


def long_function_upload2(file_handle: io.IOBase) -> {"results": str}:
    time.sleep(30)
    return {"results": "ok"}


def function_restart_on_clientworker(tmpdir, port_offset, func):
    file = __file__

    client_port = 5000 + port_offset
    broker_port = 5004 + port_offset

    ndclient_path = os.path.join(tmpdir, "ndclient.txt")
    cache_client_dir = os.path.join(tmpdir, "client")
    cache_bw_dir = os.path.join(tmpdir, "bw")
    with open(ndclient_path, "w") as f:
        f.write("localhost:{}\n".format(broker_port))
    client_app = create_p2p_client_app(ndclient_path, local_port=client_port, mongod_port=client_port + 100,
                                       cache_path=cache_client_dir)
    client_func = client_app.register_p2p_func()(func)

    broker_worker_app = P2PBrokerworkerApp(None, local_port=broker_port, mongod_port=broker_port + 100,
                                           cache_path=cache_bw_dir)
    broker_worker_app.register_p2p_func()(func)
    broker_worker_thread = ServerThread(broker_worker_app, 10)
    broker_worker_thread.start()
    while select_lru_worker(client_app.registry_functions[func.__name__]) == (None, None):
        time.sleep(3)
        print("Waiting for client to know about broker")

    ndcw_path = os.path.join(tmpdir, "ndcw.txt")
    client_worker_port = 5005 + port_offset
    cache_cw_dir = os.path.join(tmpdir, "cw")
    with open(ndcw_path, "w") as f:
        f.write("localhost:{}\n".format(broker_port))
    clientworker_app = P2PClientworkerApp(ndcw_path, local_port=client_worker_port,
                                          mongod_port=client_worker_port + 100, cache_path=cache_cw_dir)
    clientworker_app.register_p2p_func(can_do_work_func=lambda: True)(func)
    clientworker_thread = ServerThread(clientworker_app)
    clientworker_thread.start()
    while select_lru_worker(clientworker_app.registry_functions[func.__name__].p2pfunction) == (None, None):
        time.sleep(3)
        print("Waiting for clientworker to know about broker")

    with ThreadPoolExecutor(max_workers=10) as executor:
        pool_future = executor.submit(client_func, file_handle=open(file, 'rb'))
        p2p_future = pool_future.result()
        time.sleep(5)
        p2p_future.restart()
        result_ = p2p_future.get()
        assert result_["results"] == "ok"
        # assert len(list_results) == num_calls and all(isinstance(r, dict) for r in list_results)
        # print(list_results)
    client_app.background_server.shutdown()
    print("Shutdown client")
    broker_worker_thread.shutdown()
    print("Shutdown brokerworker")
    clientworker_thread.shutdown()
    print("Shutdown clientworker")
    time.sleep(3)


def function_delete_on_clientworker(tmpdir, port_offset, func):
    file = __file__

    client_port = 5000 + port_offset
    broker_port = 5004 + port_offset

    ndclient_path = os.path.join(tmpdir, "ndclient.txt")
    cache_client_dir = os.path.join(tmpdir, "client")
    cache_bw_dir = os.path.join(tmpdir, "bw")
    with open(ndclient_path, "w") as f:
        f.write("localhost:{}\n".format(broker_port))
    client_app = create_p2p_client_app(ndclient_path, local_port=client_port, mongod_port=client_port + 100,
                                       cache_path=cache_client_dir)
    client_func = client_app.register_p2p_func()(func)

    broker_worker_app = P2PBrokerworkerApp(None, local_port=broker_port, mongod_port=broker_port + 100,
                                           cache_path=cache_bw_dir)
    broker_worker_app.register_p2p_func()(func)
    broker_worker_thread = ServerThread(broker_worker_app, 10)
    broker_worker_thread.start()
    while select_lru_worker(client_app.registry_functions[func.__name__]) == (None, None):
        time.sleep(3)
        print("Waiting for client to know about broker")

    ndcw_path = os.path.join(tmpdir, "ndcw.txt")
    client_worker_port = 5005 + port_offset
    cache_cw_dir = os.path.join(tmpdir, "cw")
    with open(ndcw_path, "w") as f:
        f.write("localhost:{}\n".format(broker_port))
    clientworker_app = P2PClientworkerApp(ndcw_path, local_port=client_worker_port,
                                          mongod_port=client_worker_port + 100, cache_path=cache_cw_dir)
    clientworker_app.register_p2p_func(can_do_work_func=lambda: True)(func)
    clientworker_thread = ServerThread(clientworker_app)
    clientworker_thread.start()
    while select_lru_worker(clientworker_app.registry_functions[func.__name__].p2pfunction) == (None, None):
        time.sleep(3)
        print("Waiting for clientworker to know about broker")

    with ThreadPoolExecutor(max_workers=10) as executor:
        pool_future = executor.submit(client_func, file_handle=open(file, 'rb'))
        p2p_future = pool_future.result()
        time.sleep(5)
        p2p_future.delete()
        # assert len(list_results) == num_calls and all(isinstance(r, dict) for r in list_results)
        # print(list_results)
    col = list(MongoClient(port=broker_port + 100)["p2p"][func.__name__].find({}))
    if len(col) != 0:
        print("broker", col)
        assert False
    col = list(MongoClient(port=client_worker_port + 100)["p2p"][func.__name__].find({}))
    if len(col) != 0:
        print("worker", col)
        assert False
    client_app.background_server.shutdown()
    print("Shutdown client")
    broker_worker_thread.shutdown()
    print("Shutdown brokerworker")
    clientworker_thread.shutdown()
    print("Shutdown clientworker")
    time.sleep(3)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        testnum = 7
    else:
        testnum = int(sys.argv[1])

    if testnum==0:
        multiple_client_calls_client_worker(clean_and_create(), 0, func=large_file_function)
    elif testnum==1:
        multiple_client_calls_client_worker(clean_and_create(), 50, func=large_file_function_wait)
    elif testnum==2:
        delete_old_requests(clean_and_create(), 100, func=actual_large_file_function_wait,
                        file='/home/achellaris/big_data/torrent/torrents/Wim Hof Method/03 - Breathing/Extended breathing exercise.mp4')
    elif testnum==3:
        largef = r'/home/achellaris/big_data/torrent/torrents/The.Sopranos.S06.720p.BluRay.DD5.1.x264-DON/The.Sopranos.S06E15.Remember.When.720p.BluRay.DD5.1.x264-DON.mkv'
        largef = r'/home/achellaris/big_data/torrent/torrents/Wim Hof Method/03 - Breathing/Extended breathing exercise.mp4'
        upload_only_no_execution_multiple_large_files(clean_and_create(), 1060, func=actual_large_file_function_wait,
                            file=largef)
    elif testnum == 4:
        function_crash_on_clientworker_test(clean_and_create(), 1510, func=crashing_function,
                            file=__file__)
    elif testnum == 5:
        function_restart_unfinished_upload_on_broker(clean_and_create(), 1610, func=long_function_upload)
    elif testnum == 6:
        function_restart_on_clientworker(clean_and_create(), 2050, func=long_function_upload2)
    elif testnum == 7:
        function_delete_on_clientworker(clean_and_create(), 150, func=long_function_upload2)
    else:
        exit(-1)
    # # TODO I still need to test what happens to a request when it remains unsolved due to outside factors (power drop)
    # #  and not internal function errors that can be catched
    #
