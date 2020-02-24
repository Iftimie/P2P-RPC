# P2P-RPC
Remote Procedure Call in a Peer to Peer network

A (presumably) simple framework for distributed remote procedure call of heavy computational (GPU) and storage intensive functions.

The framework is designed to have 3 types of nodes:
* Client: nodes that actually call functions
* BrokerWorker: reachable nodes that can do work or redirect to ClientWorkers
* ClientWorker: unreachable nodes that can pull work from BrokerWorkers, execute function, then push the results back 
## Basic usage

We will have a network composed of 3 nodes. A client, a brokerworker and a clientworker. 
The scenario will be the following, we have a computationally intensive function that cannot be executed locally. The brokerworker can only redirect work. The clientworker is in a private network that cannot be reached, but can reach the broker.

### Declaring a function
Assuming we have a task that requires as input 0 or multiple files as input and may return 0 or multiple files as output we can declare a function in the following way:
* function must type annotate all its input arguments
* function must type annotate its return argument as a dictionary where the values are the returned items datatypes 
```python
import io

def analyze_large_file(video_handle: io.IOBase, arg2: int) -> {"results_file1": io.IOBase,
                                                               "results_file2": io.IOBase,
                                                               "res_var": int}:
    video_handle.close()
    return {"results_file1": open(video_handle.name, 'rb'),
            "results_file2": open(__file__, 'rb'),
            "res_var": 10}
```

### Declaring the client
In our client code we can use this function in a distributed fashion in the following way:

client.py
```python
from p2prpc.p2p_client import create_p2p_client_app
from function import analyze_large_file
import os.path as osp

password = "super secret password"
path = osp.join(osp.dirname(__file__), 'clientdb')

# create the P2PClientApp
client_app = create_p2p_client_app("network_discovery_client.txt", password=password, cache_path=path)

# decorate the function
analyze_large_file = client_app.register_p2p_func(can_do_locally_func=lambda: False)(analyze_large_file)

# call the function
res = analyze_large_file(video_handle=open(__file__, 'rb'), arg2=100)

# wait for the results
print(res.get())

client_app.background_server.shutdown()
```
We must also create a file with a list of addresses for network discovery. The client must know at least of a broker in order to dispatch work to it. If unsuccessful when connecting to a broker (network may be empty) it will execute locally.

network_discovery_client.txt
```
localhost:5001
```

### Declaring the brokerworker

For our simple case, the broker does not need a network discovery file as it does not need to reach any other node. It only needs to be reached.

```python
from p2prpc.p2p_brokerworker import P2PBrokerworkerApp
from function import analyze_large_file
import os.path as osp

password = "super secret password"
path = osp.join(osp.dirname(__file__), 'brokerworkerdb')
broker_worker_app = P2PBrokerworkerApp(None, password=password, cache_path=path)

# decorate the function
broker_worker_app.register_p2p_func(can_do_locally_func=lambda: False)(analyze_large_file)

broker_worker_app.run(host='0.0.0.0')
```

### Declaring the clientworker
```python
from p2prpc.p2p_clientworker import P2PClientworkerApp
from function import analyze_large_file
import os.path as osp

password = "super secret password"
path = osp.join(osp.dirname(__file__), 'clientworkerdb')

clientworker_app = P2PClientworkerApp("network_discovery_clientworker.txt", password=password, cache_path=path)

# decoration in this case does not return a new function
clientworker_app.register_p2p_func(can_do_work_func=lambda: True)(analyze_large_file)
clientworker_app.run(host='0.0.0.0')

```
As with the client.py we need to create a file with a list of addresses for network discovery. The clientorker will search for work, pull the input arguments, execute the function then push the output items.
It will run these steps in an infinite loop.

network_discovery_clientworker.txt
```
localhost:5001
```

### Explaining some of the concepts

#### Function type annotation
In order to serialize the arguments to the pass them between nodes we must know their datatypes. In this way I believe I simplified the implementation of the 3 types of nodes. 

I also think that the implementation is more secure as worker nodes must be aware of the function and the expected arguments.

There are a few constraints regarding the data types.
* all arguments must be type annotated
* arguments can be only primitives such as int, float, bool, string
* arguments can be files opened in 'rb' mode
* identifier, nodes, timestamp argument names are not allowed
* function must return something (that something must be the declared dictionary)
* function must be return annotated with dict (keys can only be strings and values must only be data types)


#### Client code

The function requires a cache_path that will serve both as a database and as function call caches.
For example it will save function outputs such as files received from the workers on the network to the cache_path.
In the same directory, a mondo daemon will create the database.

When the function is called, the first thing it does is to create a hash (identifier) of the arguments. 

Using the identifier, it will check whether the results have already been precomputed (we do not want to wait for another couple of hours).

If the identifier is in the local database a future object will be created that when called with .get() will return the results from the local database or will wait for the workers in the network to finish the work. 

If the identifier is not in the local database, then it will be checked if the function can be executed locally.

If the function cannot be executed locally, a broker is searched. If not broker found, then it will be executed locally.

After a broker is found, the input arguments are transferred to the broker, then a call to the function is made.

Then the future object is making calls to the broker asking for the declared keys in the return annotation.

Under the hood, all data transfer and function calls are implemented using HTTP calls.

#### BrokerWorker code

When decorating the function, the app will actually create HTTP routes for different actions required for transferring data and function calling.

The node creates a mongodb collection using the function name. In this collection it will store everything related to a function call: input arguments, output values and additional metadata such as identifier, timestamp, nodes, etc.

The node creates the following HTTP routes:
* p2p_push_update: using an identifier, it accepts new data (input arguments) or updates data (output values)
* p2p_pull_update: using an identifier, client nodes will ask for output values, and clientworkers will ask for input arguments
* execute_function: using an identifier and if the can_do_work_func() function returns True, it will start a subprocess using the received argumenst and the decorated function
* search_work: if for a specific set of input arguments no subprocess or no other clientworker has started  work, it will return the identifier

#### ClientWorker code

When decorating the function, the app will select the broker with most unfinished work (actually it's not implemented like that).
It will ask for an identifier if one, and then it will make calls to:
* p2p_pull_update to download input arguments
* call to the function using the downloaded arguments
* p2p_push_update once the function has finished
