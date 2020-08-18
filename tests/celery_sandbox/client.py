import shutil
import subprocess
import os
import func as tasks
from celery.task.control import revoke
from celery.app.control import Control
from celery.result import AsyncResult
import pymongo
import signal

#kill -9 $(ps ax | grep celery | fgrep -v grep | awk '{ print $1 }')
#kill -9 $(ps ax | grep defunct | fgrep -v grep | awk '{ print $1 }')
#kill -9 $(ps ax | grep python | fgrep -v grep | awk '{ print $1 }')
#kill -9 $(ps ax | grep mongo | fgrep -v grep | awk '{ print $1 }')

class MongoManager:
    def __init__(self):

        self.cache_path = "mongodbpath"
        if os.path.exists(self.cache_path):
            shutil.rmtree(self.cache_path)
            print("removed files. now crearing new dir")
            os.mkdir(self.cache_path)
        else:
            os.mkdir(self.cache_path)
        self.mongoprocess = None
        self.mongod_port = tasks.mongodport

    def start(self):
        # from p2prpc.base.P2PFlaskApp.start_background_threads
        self.mongodprocess = subprocess.Popen(["mongod", "--dbpath", self.cache_path, "--port", str(self.mongod_port),
                                          "--logpath", os.path.join(self.cache_path, "mongodlog.log")])

    def kill(self):
        self.mongodprocess.kill()

    def wait_for_mongo_online(self):
        while True:
            try:
                client = pymongo.MongoClient(port=self.mongod_port)
                client.server_info()
                break
            except:
                print("Mongo d not online yet")

# mm = MongoManager()
# mm.start()
# mm.wait_for_mongo_online()


print("Everythinh started. Waiting for task finishing")
ans = tasks.hello.delay()
import time
print("Waiting 5 seconds before revoking")
time.sleep(5)

celery_control = Control(tasks.app)
celery_control.revoke(ans.id, terminate=True, signal=signal.SIGHUP)
revoke(ans.id, terminate=True, signal=signal.SIGHUP)
tasks.app.control.revoke(ans.id, terminate=True, signal=signal.SIGHUP)
ans.revoke(terminate=True, signal=signal.SIGHUP)
tasks.app.control.revoke(ans.id, terminate=True, signal='SIGKILL')
AsyncResult(ans.id, app=tasks.app).revoke(terminate=True, signal='SIGKILL')
ans.revoke(terminate=True, signal='SIGKILL')
print("Revoked function")
print(ans.get())

# mm.kill()

