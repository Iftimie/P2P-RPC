from .base import P2PFunction, P2PArguments


class ClientNoBrokerFound(Exception):
    """
    Exception raised when no available broker is found in the network.
    """
    def __init__(self, p2pfunction: P2PFunction, p2parguments: P2PArguments):
        self.p2pfunction = p2pfunction
        self.p2parguments = p2parguments

    def __str__(self):
        return f"No broker found for function {self.p2pfunction.function_name} with arguments identifier {self.p2parguments.args_identifier}"


class ClientFunctionDifferentBytecode(Exception):
    """
    Exception raised when a client has a function that has a different bytecode compared to the one found in broker
    """
    def __init__(self, p2pfunction: P2PFunction, broker_addr):
        self.p2pfunction = p2pfunction
        self.broker_addr = broker_addr

    def __str__(self):
        return f"Function {self.p2pfunction.function_name} in broker {self.broker_addr} has different bytecode compared to local function"


class ClientFunctionError(Exception):
    """
    Exception raised when the client receives information that the function execution on a worker has crashed. It can
    happen when the function raises an error. When this is acknowledged, the exception is mirrored
    """
    def __init__(self, p2pfunction: P2PFunction, p2parguments: P2PArguments):
        self.p2pfunction = p2pfunction
        self.p2parguments = p2parguments

    def __str__(self):
        return f"Function {self.p2pfunction.function_name} with arguments identifier {self.p2parguments.args_identifier} crashed on worker"


class ClientFutureTimeoutError(Exception):
    """
    Exception raised when the client waits too long for a future to finish. This may happen due to long upload time
    of the arguments or due to long execution time. It can also happen if worker is suddenly disconnected and cannot send
    updates about him failing (the worker cannot send information so that ClientFunctionError is thrown)
    """
    def __init__(self, p2pfunction: P2PFunction, p2parguments: P2PArguments):
        self.p2pfunction = p2pfunction
        self.p2parguments = p2parguments

    def __str__(self):
        return f"Function {self.p2pfunction.function_name} with arguments identifier {self.p2parguments.args_identifier} future timeout"


class ClientUnableFunctionTermination(Exception):
    """
    Exception raised when the client calls restart or terminate on a future, but the information is not received by the
    worker or the worker is unable to send back the information that the function is truly finished
    """
    def __init__(self, p2pfunction: P2PFunction, p2parguments: P2PArguments):
        self.p2pfunction = p2pfunction
        self.p2parguments = p2parguments

    def __str__(self):
        return f"Function {self.p2pfunction.function_name} with arguments identifier {self.p2parguments.args_identifier} unable to properly terminate"


class ClientUnableFunctionDeletion(Exception):
    """
    Exception raised when the client calls delete on a future, but the information is not received by the
    worker or the worker is unable to send back the information that the function arguments is truly deleted
    """
    def __init__(self, p2pfunction: P2PFunction, p2parguments: P2PArguments):
        self.p2pfunction = p2pfunction
        self.p2parguments = p2parguments

    def __str__(self):
        return f"Function {self.p2pfunction.function_name} with arguments identifier {self.p2parguments.args_identifier} unable to properly delete arguments"


class ClientP2PFunctionInvalidArguments(Exception):
    def __init__(self, p2pfunction, message):
        self.p2pfunction = p2pfunction
        self.message = message

    def __str__(self):
        return f"Function {self.p2pfunction.function_name} received invalid arguments. Message is {self.message}"


class ClientHashCollision(Exception):
    def __init__(self, p2pfunction, collection, kwargs):
        self.p2pfunction=p2pfunction
        self.collection = [i['identifier'] for i in collection]
        self.kwargs = kwargs

    def __str__(self):
        return f"Function {self.p2pfunction.function_name} received kwargs {self.kwargs} that resulted in hash collision with identifiers {self.collection}"


class Broker2ClientIdentifierNotFound(Exception):
    """
    Exception raised when the response from the broker to the client results in identifier not found
    """
    def __init__(self, func_name, identifier):
        self.func_name = func_name
        self.identifier = identifier

    def __str__(self):
        return f"Function {self.func_name} resulted in identifier not found for {self.identifier}"


class WorkerInvalidResults(Exception):
    """
    Exception raised when the results of executing a function do not respect the convention of this p2p framework.
    Which is to respect the declared keys as strings and their value datatypes
    """
    def __init__(self, p2pfunction, p2parguments, msg):
        self.p2pfunction=p2pfunction
        self.p2parguments=p2parguments
        self.msg = msg

    def __str__(self):
        return f"Function {self.p2pfunction.function_name} with identifier {self.p2parguments.args_identifier} has return error:\n" \
               f"{self.msg}"


class P2PDataSerializationError(Exception):
    def __init__(self, msg):
        self.msg=msg

    def __str__(self):
        return self.msg


class P2PDataInvalidDocument(Exception):
    def __init__(self, msg):
        self.msg=msg

    def __str__(self):
        return self.msg


class P2PDataHashCollision(Exception):
    def __init__(self, msg):
        self.msg=msg

    def __str__(self):
        return self.msg