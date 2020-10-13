# P2P-RPC
Remote Procedure Call in a Peer to Peer network

A (presumably) simple framework for distributed remote procedure call of heavy computational (GPU) and storage intensive functions.

The framework is designed to have 3 types of nodes:
* Client: nodes that actually call functions
* Broker: reachable nodes that can do work or redirect to Workers
* Worker: unreachable nodes that can pull work from Brokers, execute function, then push the results back 
## Basic usage

We will have a network composed of 3 nodes. A client, a broker and a worker. 
The scenario will be the following, we have a computationally intensive function that cannot be executed locally. The broker can only redirect work. The worker is in a private network that cannot be reached, but can reach the broker.

### Declaring a function
Assuming we have a task that requires as input 0 or multiple files as input and may return 0 or multiple files as output we can declare a function in the following way:
* function must type annotate all its input arguments
* function must type annotate its return argument as a dictionary where the values are the returned items datatypes 
* function name must have "p2prpc_" prefix

`function.py`
```python
import io

def p2prpc_analyze_large_file(video_handle: io.IOBase, arg2: int) -> {"results_file1": io.IOBase,
                                                               "results_file2": io.IOBase,
                                                               "res_var": int}:
    video_handle.close()
    return {"results_file1": open(video_handle.name, 'rb'),
            "results_file2": open(__file__, 'rb'),
            "res_var": 10 * arg2}
```

### Declaring the broker
The package comes in with a CLI that helps generating the basic code.
To run a simple demo on the local environment we need to start with the broker code.
We start by executing the following commands:


```
p2prpc generate-broker function.py
sudo docker-compose -f broker/broker.docker-compose.yml up -d
sudo docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' broker-discovery | tr -d "\n" > 'discovery.txt'; echo ':5002' >> 'discovery.txt'
``` 
The first command will generate the code for broker in a new directory named broker/. It will receive as argument the filename where the function is declared.
The new directory will contain the docker-compose file used for starting the services that make up the broker and it will also contain the generated brokerapp.py file. These files will not require any additional modifications.

The second command will start the services.

The last command will find out the IP for the Bookkeeper (Network discovery) service and will create a new file named 'discovery.txt'. This new file will be used by worker and clients to find out about broker.  

### Declaring the worker
The worker needs to know about broker. When the code is generated for worker, it also needs as an argument the previously created 'discovery.txt' file.
```
p2prpc generate-worker function.py discovery.txt
sudo docker-compose -f broker/broker.docker-compose.yml up -d
```


### Declaring the client
The client is declared in a similar manner as the worker. This command will also generate a basic template for using the function in a distributed manner. 
```
p2prpc generate-client function.py discovery.txt
```
After running the command a new client/ directory should be generated, and inside there should be a clientapp.py file. The file will be modified according to the user needs.

In our client code we can use this function in a distributed fashion in the following way:

`client/clientapp.py`
```python
from p2prpc.p2p_client import create_p2p_client_app
from function import p2prpc_analyze_large_file
import os.path as osp
import logging

logger = logging.getLogger(__name__)

client_app = create_p2p_client_app("discovery.txt", password="super secret password", cache_path=osp.join(osp.abspath(osp.dirname(__file__)), 'clientdb'))

p2prpc_analyze_large_file = client_app.register_p2p_func()(p2prpc_analyze_large_file)

res = p2prpc_analyze_large_file(video_handle=open(__file__, 'rb'), arg2=160)

print(res.get())

client_app.background_server.shutdown()
```

#### Additional function features
When having long running functions, we may want to know about their progress. Thus we can import p2p_progress_hook and call it with current index and end index. The result will be a progress key in database with range 0-100%
```python
import io
from p2prpc.p2p_client import p2p_progress_hook
import time

def p2prpc_analyze_large_file(video_handle: io.IOBase, arg2: int) -> {"results_file1": io.IOBase,
                                                               "results_file2": io.IOBase,
                                                               "res_var": int}:
    video_handle.close()
    for i in range(100):
        p2p_progress_hook(i, 100)
        time.sleep(1)
    return {"results_file1": open(video_handle.name, 'rb'),
            "results_file2": open(__file__, 'rb'),
            "res_var": 10 * arg2}
```

### More Examples
In example_better/ directory there is a Makefile that contains more commands for starting, recreating the services, finding their IPs, etc.
Also in the tests/docker_sandbox tests/scenario and tests/scenario2 there are some examples. 

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

The function requires a cache_path that will be used to store the results from calls in case there are files returned.

When the function is called, the first thing it does is to create a hash (identifier) of the arguments. 

Using the identifier, it will check whether the results have already been precomputed (we do not want to wait for another couple of hours).

If the identifier is in the local database a future object will be created that when called with .get() will return the results from the local database or will wait for the workers in the network to finish the work. 

Otherwise, a broker is searched. If not broker is found after a few trials, then it will raise an error.

After a broker is found, the input arguments are transferred to the broker, and broker is notified that there it should allow workers to work on the new data.

Then the future object is making calls to the broker asking for the declared keys in the return annotation.

Under the hood, all data transfer and function calls are implemented using HTTP calls.

#### BrokerWorker code

When decorating the function, the app will actually create HTTP routes for different actions required for transferring data and function calling.

The node creates a mongodb collection using the function name. In this collection it will store everything related to a function call: input arguments, output values and additional metadata such as identifier, timestamp, nodes, etc.

The node creates the following HTTP routes among others:
* p2p_push_update: using an identifier, it accepts new data (input arguments) or updates data (output values)
* p2p_pull_update: using an identifier, client nodes will ask for output values, and clientworkers will ask for input arguments
* execute_function: using an identifier and if the can_do_work_func() function returns True ( a GPU is available ), it will start a subprocess using the received argumenst and the decorated function
* search_work: if for a specific set of input arguments no subprocess or no other clientworker has started  work, it will return the identifier
* terminate_function: allows client to issue a terminating signal, so that the functional will stop
* delete_function: allows client to issue a delete signal, so that the stored data will be deleted. terminate_function is called before this

#### ClientWorker code

When decorating the function, the app will select one broker that has work available.
It will ask for an identifier if one, and then it will make calls to:
* p2p_pull_update to download input arguments
* call to the function using the downloaded arguments
* p2p_push_update once the function has finished


#### Hidden stuff
All 3 nodes a mongodb service, a bookkeeping service and the service itself.

There are also some background threads that make updates such as deleting data in case it expired.

## Installation
Source based installation, maybe a distribution will be published on pypi.
```
pip3 install git+git://github.com/Iftimie/P2P-RPC.git
```