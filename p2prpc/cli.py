import click
import sys
import os
from p2prpc.code_generation.broker import dockercompose_string, broker_script
from p2prpc.code_generation.client import client_dockercompose_string, client_app_template
from p2prpc.code_generation.worker import worker_dockerfile_string, workerapp_string

import collections
import p2prpc


@click.group()
def cli():
    pass


def get_p2p_functions(filename):
    # p2prpc generate-broker function.py
    modulecontent = open(filename).read()
    eval(compile(modulecontent, "<string>", 'exec'))
    functions = []
    locals_ = locals()
    for var_name in dir():
        if var_name.startswith("p2prpc_") and var_name != "p2prpc_" and isinstance(locals_[var_name],
                                                                                   collections.Callable):
            functions.append(locals_[var_name])
    if not functions or len(functions)>1:
        raise ValueError("Only 1 function is supported at the moment. Make sure the function name has 'p2prpc_' prefix")
    return functions


@cli.command()
@click.argument('filename')
@click.argument('password', default='super secret password')
def generate_broker(filename, password):
    click.echo('Code generation for broker started')
    if not os.path.exists("broker"):
        os.mkdir("broker")


    module_name = filename.split('.')[0]
    updated_broker_script_path = "broker/brokerapp.py"
    with open(updated_broker_script_path, "w") as f:
        updated_broker_script = broker_script.format(module=module_name,
                                                     function=get_p2p_functions(filename)[0].__name__,
                                                     super_secret_password=password)
        f.write(updated_broker_script)

    p2prpc_package_path = os.path.dirname(p2prpc.__file__)
    updated_broker_script_path = os.path.abspath(updated_broker_script_path)
    filepath = os.path.abspath(filename)
    print(filepath)
    with open("broker/broker.docker-compose.yml", "w") as f:
        updated_dockercompose_string = dockercompose_string.format(p2prpc_package_path=p2prpc_package_path,
                                                                   super_secret_password=password,
                                                                   updated_broker_script_path=updated_broker_script_path,
                                                                   current_function_file_path=filepath)
        f.write(updated_dockercompose_string)

    click.echo('Code generation for broker finished')
    click.echo('Run: sudo docker-compose -f broker/broker.docker-compose.yml up')


@cli.command()
@click.argument('filename')
@click.argument('networkdiscovery')
@click.argument('password', default='super secret password')
@click.option('--overwrite', is_flag=True)
def generate_client(filename, networkdiscovery, password, overwrite):
    click.echo('Code generation for client')
    if not os.path.exists("client"):
        os.mkdir("client")


    module_name = filename.split('.')[0]
    updated_client_script_path = "client/clientapp.py"
    if not os.path.exists(updated_client_script_path) or overwrite:
        with open(updated_client_script_path, "w") as f:
            updated_client_app_template = client_app_template.format(module=module_name,
                                                         function=get_p2p_functions(filename)[0].__name__,
                                                         super_secret_password=password)
            f.write(updated_client_app_template)

    p2prpc_package_path = os.path.dirname(p2prpc.__file__)
    networkdiscovery = os.path.abspath(networkdiscovery)
    with open("client/client.docker-compose.yml", "w") as f:
        updated_dockercompose_string = client_dockercompose_string.format(p2prpc_package_path=p2prpc_package_path,
                                                                   super_secret_password=password,
                                                                   network_discovery_file=networkdiscovery)
        f.write(updated_dockercompose_string)

    click.echo('Code generation for client finished')
    click.echo('Run: sudo docker-compose -f client/client.docker-compose.yml up')


@cli.command()
@click.argument('filename')
@click.argument('networkdiscovery')
@click.argument('password', default='super secret password')
def generate_worker(filename, networkdiscovery, password):
    click.echo('Code generation for worker')
    if not os.path.exists("worker"):
        os.mkdir("worker")

    module_name = filename.split('.')[0]
    updated_worker_script_path = "worker/workerapp.py"
    with open(updated_worker_script_path, "w") as f:
        updated_workerapp_string = workerapp_string.format(module=module_name,
                                                     function=get_p2p_functions(filename)[0].__name__,
                                                     super_secret_password=password)
        f.write(updated_workerapp_string)

    p2prpc_package_path = os.path.dirname(p2prpc.__file__)
    updated_worker_script_path = os.path.abspath(updated_worker_script_path)
    filepath = os.path.abspath(filename)
    networkdiscovery = os.path.abspath(networkdiscovery)
    with open("worker/worker.docker-compose.yml", "w") as f:
        updated_dockercompose_string = worker_dockerfile_string.format(p2prpc_package_path=p2prpc_package_path,
                                                                   super_secret_password=password,
                                                                   worker_app_path=updated_worker_script_path,
                                                                   network_discovery_file=networkdiscovery,
                                                                   current_function_file_path=filepath)
        f.write(updated_dockercompose_string)

    click.echo('Code generation for worker finished')
    click.echo('Run: sudo docker-compose -f worker/worker.docker-compose.yml up')


def main():
    cli(args=sys.argv[1:], prog_name="python -m p2prpc")


if __name__ == '__main__':
    main()

