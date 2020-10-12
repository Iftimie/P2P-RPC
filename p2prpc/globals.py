from functools import partial
import requests

requests.post = partial(requests.post, timeout=5)
longpost = partial(requests.post, timeout=5) # i need to check if this is necessary
requests.get = partial(requests.get, timeout=5)

future_timeout = 3600*24  # seconds
future_sleeptime = 4  # seconds
