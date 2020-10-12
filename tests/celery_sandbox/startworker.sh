# it is important that both the client and the worker have the same name resolution for a function
# given the following directory structure
# - tests \
#   - celery_sandbox \
#     - func.py

# if current directory is p2p-rpc/tests/celery_sandbox/func.py and I start the worker as such
# celery -A func worker -l info

# it cannot receive tasks from a client that has working directory p2p-rpc and imports the function as
# import tests.celery_sandbox.func
# func.hello.delay()

# the worker will give key error KeyError: 'tests.celery_sandbox.func.hello'


# thus both the worker and the client must have same working directory and import the tasks in a similar manner
# worker
# cur_dir  p2p-rpc
# celery -A tests.celery_sandbox.func worker -l info

# client
# curdir p2p-rpc
# import tests.celery_sandbox.func
# func.hello.delay()

# since this is a sandbox and both scrips (startworker.sh and startmongoandclient.sh) are in the same folder I will open
# them as such

celery -A func worker -l DEBUG --statedb=worker.state --pool=prefork --concurrency=3
# CTRL Z to kill