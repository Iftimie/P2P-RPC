from functools import partial
import requests

requests.post = partial(requests.post, timeout=5)
longpost = partial(requests.post, timeout=120) # i need to check if this is necessary
requests.get = partial(requests.get, timeout=5)
