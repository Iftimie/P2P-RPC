import click
import sys
import os
from p2prpc.code_generation.broker import dockercompose_string, broker_script
from p2prpc.code_generation.client import client_dockercompose_string, client_app_template
from p2prpc.code_generation.worker import worker_dockerfile_string, workerapp_string
from p2prpc.code_generation.Dockerfile import dockerfile_string
import shutil
import collections
import p2prpc


@click.group()
def cli():
    pass


def copy_and_overwrite(from_path, to_path):
    if os.path.exists(to_path):
        shutil.rmtree(to_path)
    shutil.copytree(from_path, to_path)


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


def create_volumes_string(volumes, apptype):
    if os.path.exists("install_deps.sh"):
        volumes = volumes + (os.path.abspath("install_deps.sh"),) # TODO. this is stupid. there should simply be another dockerfile available for modification. too much time is wasted by installing everytime the dependencies
    volume_string = []
    for vol in volumes:
        last_path = os.path.split(vol)[-1]
        newline = '      - ' + os.path.abspath(vol) + ":/app/"+apptype+"/"+last_path
        volume_string.append(newline)
    return '\n'.join(volume_string)

@cli.command()
@click.argument('filename')
@click.option('--password', default='super secret password')
def generate_broker(filename, password):
    click.echo('Code generation for broker started')
    if not os.path.exists("broker"):
        os.mkdir("broker")

    sys.path.insert(0, os.getcwd())  # insert this so that any relative import done by file can be executed successfully

    module_name = filename.split('.')[0].replace('/','.').replace('\\', '.')
    updated_broker_script_path = "broker/brokerapp.py"
    with open(updated_broker_script_path, "w") as f:
        updated_broker_script = broker_script.format(module=module_name,
                                                     function=get_p2p_functions(filename)[0].__name__,
                                                     super_secret_password=password)
        f.write(updated_broker_script)

    p2prpc_package_path = os.path.dirname(p2prpc.__file__)
    # TODO once the package is stable, it should not copy anything to broker/ folder/ it should pip install p2prpc
    # if not os.path.exists('broker/p2prpc'):
    copy_and_overwrite(p2prpc_package_path, 'broker/p2prpc')
    destination_dockerfile_path = os.path.join('broker/Dockerfile')
    with open(destination_dockerfile_path, 'w') as f:
        f.write(dockerfile_string.format(p2prpc_path=p2prpc_package_path,
                                         local_project=os.getcwd(),
                                         apptype='broker'))
    destination_dockerfile_path = os.path.abspath(destination_dockerfile_path)

    filepath = os.path.abspath(filename)
    with open("broker/broker.docker-compose.yml", "w") as f:
        updated_dockercompose_string = dockercompose_string.format(
                                                                   super_secret_password=password,
                                                                   dockerfile_path=destination_dockerfile_path,
                                                                   docker_context=os.getcwd())
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

    sys.path.insert(0, os.getcwd())  # insert this so that any relative import done by file can be executed successfully

    module_name = filename.split('.')[0].replace('/','.').replace('\\', '.')
    updated_client_script_path = "client/clientapp.py"
    if not os.path.exists(updated_client_script_path) or overwrite:
        with open(updated_client_script_path, "w") as f:
            updated_client_app_template = client_app_template.format(module=module_name,
                                                         function=get_p2p_functions(filename)[0].__name__,
                                                         super_secret_password=password)
            f.write(updated_client_app_template)

    p2prpc_package_path = os.path.dirname(p2prpc.__file__)
    # TODO once the package is stable, it should not copy anything to broker/ folder/ it should pip install p2prpc
    # if not os.path.exists('client/p2prpc'):
    copy_and_overwrite(p2prpc_package_path, 'client/p2prpc')
    destination_dockerfile_path = os.path.join('client/Dockerfile')
    with open(destination_dockerfile_path, 'w') as f:
        f.write(dockerfile_string.format(p2prpc_path=p2prpc_package_path,
                                         local_project=os.getcwd(),
                                         apptype='client'))
    networkdiscovery = os.path.abspath(networkdiscovery)
    filepath = os.path.abspath(filename)
    with open("client/client.docker-compose.yml", "w") as f:
        updated_dockercompose_string = client_dockercompose_string.format(
                                                                   super_secret_password=password,
                                                                    dockerfile_path=destination_dockerfile_path,
                                                                    docker_context=os.getcwd(),
                                                                   network_discovery_file=networkdiscovery)
        f.write(updated_dockercompose_string)

    click.echo('Code generation for client finished')
    click.echo('Run: sudo docker-compose -f client/client.docker-compose.yml up')


@cli.command()
@click.argument('filename')
@click.argument('networkdiscovery')
@click.option('--password', default='super secret password')
def generate_worker(filename, networkdiscovery, password):
    click.echo('Code generation for worker')
    if not os.path.exists("worker"):
        os.mkdir("worker")

    sys.path.insert(0, os.getcwd())  # insert this so that any relative import done by file can be executed successfully

    module_name = filename.split('.')[0].replace('/','.').replace('\\', '.')
    updated_worker_script_path = "worker/workerapp.py"
    with open(updated_worker_script_path, "w") as f:
        updated_workerapp_string = workerapp_string.format(module=module_name,
                                                     function=get_p2p_functions(filename)[0].__name__,
                                                     super_secret_password=password)
        f.write(updated_workerapp_string)

    p2prpc_package_path = os.path.dirname(p2prpc.__file__)
    # TODO once the package is stable, it should not copy anything to broker/ folder/ it should pip install p2prpc
    # if not os.path.exists('worker/p2prpc'):
    copy_and_overwrite(p2prpc_package_path, 'worker/p2prpc')
    destination_dockerfile_path = os.path.join('worker/Dockerfile')
    with open(destination_dockerfile_path, 'w') as f:
        f.write(dockerfile_string.format(p2prpc_path=p2prpc_package_path,
                                         local_project=os.getcwd(),
                                         apptype='worker'))

    filepath = os.path.abspath(filename)
    networkdiscovery = os.path.abspath(networkdiscovery)
    with open("worker/worker.docker-compose.yml", "w") as f:
        updated_dockercompose_string = worker_dockerfile_string.format(super_secret_password=password,
                                                                       network_discovery_file=networkdiscovery,
                                                                       dockerfile_path=destination_dockerfile_path,
                                                                       docker_context=os.getcwd())
        f.write(updated_dockercompose_string)

    click.echo('Code generation for worker finished')
    click.echo('Run: sudo docker-compose -f worker/worker.docker-compose.yml up')


def main():
    cli(args=sys.argv[1:], prog_name="python -m p2prpc")


if __name__ == '__main__':
    main()

