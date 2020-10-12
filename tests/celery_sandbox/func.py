from celery import Celery
import time

# this declaration here is important only for the worker
mongodport = 27022
# app = Celery('project_Alex', broker=f'mongodb://localhost:{mongodport}/database_name', backend=f'mongodb://localhost:{mongodport}')
redisport = 6379
app = Celery('project_Alex', broker=f'redis://localhost:{redisport}/0', backend=f'redis://localhost:{redisport}')
# This is annoying. every time I want to do something that I need, the package does not support
# I need to programatically update the list of workers.
# that means I need a script that will run the command celery -A func worker --pool=prefork blabla
# and the same script should run the bookkeeping. And when new queues appear, the script should kill the current worker
# and update the list
# then start again the worker

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


pass
print("asdasd")
