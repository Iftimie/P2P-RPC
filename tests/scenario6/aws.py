import logging
import time
import urllib.request
import boto3
import os
from botocore.exceptions import ClientError
import paramiko
import click
import json
client = boto3.client('ec2')
ec2 = boto3.resource('ec2')

@click.group()
def cli():
    pass

def delete_key_pair(key_name, key_file_name):
    try:
        ec2.KeyPair(key_name).delete()
        os.remove(key_file_name)
    except ClientError:
        raise


def delete_security_group(group_id):
    try:
        ec2.SecurityGroup(group_id).delete()
    except ClientError:
        raise


def terminate_instance(instance_id):
    try:
        ec2.Instance(instance_id).terminate()
    except ClientError:
        logging.exception("Couldn't terminate instance %s.", instance_id)
        raise


def create_key_pair():
    key_name = 'demo-ec2-key-1604263147.1798422'
    private_key_file_name = 'demo-key-file.pem'

    if not os.path.exists(private_key_file_name):
        try:
            key_pair = ec2.create_key_pair(KeyName=key_name)
            if private_key_file_name is not None:
                with open(private_key_file_name, 'w') as pk_file:
                    pk_file.write(key_pair.key_material)
                os.system('chmod 400 demo-key-file.pem')
        except ClientError as e:
            raise e
        else:
            return key_pair
    else:
        key_pair_info = ec2.KeyPair(key_name) # it calls the underlying EC2.Client.describe_key_pairs()
        return key_pair_info


def setup_security_group(group_description):

    ans = client.describe_security_groups()
    secgroups = ans['SecurityGroups']
    if len(secgroups):
        return ec2.SecurityGroup(secgroups[0]['GroupId'])
    else:
        try:
            default_vpc = list(ec2.vpcs.filter(
                Filters=[{'Name': 'isDefault', 'Values': ['true']}]))[0]
        except ClientError:
            raise
        except IndexError:
            raise

        try:
            group_name = make_unique_name('ssh-group')
            security_group = default_vpc.create_security_group(
                GroupName=group_name, Description=group_description)
        except ClientError:
            raise

        try:
            current_ip_address = urllib.request.urlopen('http://checkip.amazonaws.com') \
                .read().decode('utf-8').strip()
            ip_permissions = list([{
                # HTTP ingress open to anyone
                'IpProtocol': 'tcp', 'FromPort': 80, 'ToPort': 80,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            }, {
                # HTTPS ingress open to anyone
                'IpProtocol': 'tcp', 'FromPort': 443, 'ToPort': 443,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            }, {
                # HTTPS ingress open to anyone
                'IpProtocol': 'tcp', 'FromPort': 5001, 'ToPort': 5002,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            }, {
                # HTTPS ingress open to anyone
                'IpProtocol': 'icmp', 'FromPort': -1, 'ToPort': -1,
                'IpRanges': [{'CidrIp': '0.0.0.0/0'}]
            }])
            ip_permissions.append({
                # SSH ingress open to only the specified IP address
                'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22,
                'IpRanges': [{'CidrIp': f'{current_ip_address}/32'}]})
            security_group.authorize_ingress(IpPermissions=ip_permissions)
        except ClientError:
            raise
        else:
            return security_group



def create_instance(image_id, instance_type, key_name, security_group_names=None):
    try:
        instance_params = {
            'ImageId': image_id, 'InstanceType': instance_type, 'KeyName': key_name
        }
        if security_group_names is not None:
            instance_params['SecurityGroups'] = security_group_names
        instance = ec2.create_instances(**instance_params, MinCount=1, MaxCount=1)[0]
    except ClientError:
        logging.exception(
            "Couldn't create instance with image %s, instance type %s, and key %s.",
            image_id, instance_type, key_name)
        raise
    else:
        return instance


def make_unique_name(name):
    return f'demo-ec2-{name}-{time.time()}'


def setup_demo():
    ssh_sec_group = setup_security_group(f'Demo group that allows SSH from owner ip address.')
    key_pair = create_key_pair()
    if not os.path.exists(".instances.json"):
        ssh_instance_broker = create_instance("ami-05c424d59413a2876", 't2.micro', key_pair.key_name, (ssh_sec_group.group_name,))
        ssh_instance_worker = create_instance("ami-05c424d59413a2876", 't2.micro', key_pair.key_name, (ssh_sec_group.group_name,))

        print(f"Waiting for instances to start...")
        ssh_instance_broker.wait_until_running()
        ssh_instance_worker.wait_until_running()
        instances = {"broker": ssh_instance_broker._id,
                     "worker": ssh_instance_worker._id}
        with open(".instances.json", "w") as json_file:
            json.dump(instances, json_file)
    else:
        with open(".instances.json") as json_file:
            instances = json.load(json_file)
        ssh_instance_broker = ec2.Instance(instances['broker'])
        ssh_instance_worker = ec2.Instance(instances['worker'])

    return (ssh_instance_broker, ssh_instance_worker), (ssh_sec_group, ), key_pair


def management_demo(ssh_instance):
    ssh_instance.load()
    print(f"At this point, you can SSH to instance {ssh_instance.instance_id} "
          f"at another command prompt by running")
    print(f"\tssh -i demo-key-file.pem ubuntu@{ssh_instance.public_ip_address}")
    os.system('chmod 400 demo-key-file.pem')
    with open('ip.txt', 'w') as f:
        f.write(ssh_instance.public_ip_address)
    input("Press Enter when you're ready to continue the demo.")


def teardown_demo(instances):

    for instance in instances:
        terminate_instance(instance.instance_id)
        instance.wait_until_terminated()
    print("Terminated the demo instances.")


def run_commands(c, command_list):
    for command in command_list:
        stdin, stdout, stderr = c.exec_command(command, get_pty=True)
        # stdout._set_mode('b')
        for line in iter(lambda: stdout.readline(2048), ""):
            print(line, end='')
        for line in stderr.readlines():
            print(line, end='')
        exit_status = stdout.channel.recv_exit_status()
        if exit_status == 0:
            print("Command finished")
        else:
            print("Error in command", exit_status)


def ssh_login():
    try:
        cert = paramiko.RSAKey.from_private_key_file('demo-key-file.pem')
        c = paramiko.SSHClient()
        c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        with open('ip.txt', 'r') as f:
            ip = f.read()
        c.connect(hostname=ip, username='ubuntu', pkey=cert)
        run_commands(c, ['git clone https://github.com/Iftimie/TruckMonitoringSystem.git',
                         'cd TruckMonitoringSystem; git checkout develop',
                         'cd TruckMonitoringSystem; git pull',
                         'cd TruckMonitoringSystem; ls',
                         'cd TruckMonitoringSystem; sudo bash install_deps_host.sh',
                         'cd TruckMonitoringSystem; sudo bash run_broker.sh'])
        c.close()

    except Exception as e:
        print("Connection Failed!!!")


@cli.command()
def run_demos():

    instances, security_groups, key_pair = setup_demo()
    management_demo(*instances)

    teardown_demo(instances)


@cli.command()
def instances():
    for i in ec2.instances.all():
        print(i)

def main():
    cli()

if __name__ == '__main__':
    main()