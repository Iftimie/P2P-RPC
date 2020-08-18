from celery import Celery
import time

# this declaration here is important only for the worker
mongodport = 27022
# app = Celery('project_Alex', broker=f'mongodb://localhost:{mongodport}/database_name', backend=f'mongodb://localhost:{mongodport}')
redisport = 6379
app = Celery('project_Alex', broker=f'redis://localhost:{redisport}/0', backend=f'redis://localhost:{redisport}')
# it should connect to the broker

# app.config_from_object('celeryconfig')

@app.task
def hello(): # the filter should be the argument
    # the filter should also contain the name of the function that needs to be executed
    # what it should do here. it should download the arguments first
    print("Task executing")
    for i in range(15):
        time.sleep(1)
        print("Still not killed")
    return 'hello world'

