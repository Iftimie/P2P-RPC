import shutil
import subprocess
import os
import func as tasks


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
        self.mongod_port = 27017

    def start(self):
        # from p2prpc.base.P2PFlaskApp.start_background_threads
        self.mongodprocess = subprocess.Popen(["mongod", "--dbpath", self.cache_path, "--port", str(self.mongod_port),
                                          "--logpath", os.path.join(self.cache_path, "mongodlog.log")])

    def kill(self):
        self.mongodprocess.kill()

mm = MongoManager()
mm.start()


print("Everythinh started. Waiting for task finishing")
ans = tasks.hello.delay()
print(ans.get())

mm.kill()

