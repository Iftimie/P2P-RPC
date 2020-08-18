from celery import Celery

# this declaration here is important only for the worker
app = Celery('project_Alex', broker='mongodb://localhost:27017/database_name', backend='mongodb://localhost:27017')
# it should connect to the broker

@app.task
def hello(): # the filter should be the argument
    # what it should do here. it should download the arguments first
    print("Task executing")
    return 'hello world'

