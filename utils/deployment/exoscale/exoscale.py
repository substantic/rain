import json
from cs import CloudStack, read_config
import argparse
import os
import paramiko
import base64
import multiprocessing
from collections import OrderedDict


def print_pretty(json_data):
    print(json.dumps(json_data, indent=4, sort_keys=True))


cs = CloudStack(**read_config())

OFFERINGS = {o["name"].lower(): o["id"] for o in cs.listServiceOfferings()["serviceoffering"]}
ZONES = {z["name"].lower(): z["id"] for z in cs.listZones()["zone"]}
SSH_KEY_PAIRS = [kp["name"] for kp in cs.listSSHKeyPairs()["sshkeypair"]]

VIRTUAL_MACHINES = cs.listVirtualMachines()
TEMPLATE_OFFERINGS = cs.listTemplates(templatefilter="executable")
NETWORK_OFFERINGS = cs.listNetworkOfferings()
SECURITY_GROUPS = cs.listSecurityGroups()
INIT_SCRIPT = """
#cloud-config
runcmd:
    - apt-get update
    - apt-get install -y python3-pip
""" # noqa
SSH_CMD = "ssh -o StrictHostKeyChecking=no"
SSH_USERNAME = "ubuntu"
SSH_PORT = 22


def create_ssh_session(hostname):
    s = paramiko.SSHClient()
    s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    s.connect(hostname, SSH_PORT, SSH_USERNAME)
    return s


def create(args):
    print("Creating...")
    vms = []
    for i in range(args.n):
        vms.append(cs.deployVirtualMachine(
            name="{}-{}".format(args.name, i),
            serviceOfferingId=OFFERINGS[args.offering],
            # templateId="4fedad2b-e96c-4a70-95f7-a9142995dba4",
            templateId="709687a4-35a7-4bfe-af24-aa00f3f391e8",  # Ubuntu 17.10
            zoneId=ZONES[args.zone],
            userdata=base64.b64encode(INIT_SCRIPT.encode("utf-8")),
            keypair=args.keypair))
    if args.env:
        env_filename = args.env
    else:
        env_filename = "{}.env".format(args.name)
    with open(env_filename, "w") as f:
        f.write(json.dumps(vms))
    print("Created ({})".format(env_filename))


def destroy(args):
    print("Destroying...")
    with open(args.env, "r") as f:
        vms = json.loads(f.read())
        for vm in vms:
            cs.destroyVirtualMachine(id=vm["id"])
    os.remove(args.env)
    print("Destroyed")


def get_nodes(args):
    with open(args.env, "r") as f:
        ids = [vm["id"] for vm in json.loads(f.read())]
        ips = OrderedDict((vm["name"], vm["nic"][0]["ipaddress"])
                          for vm in cs.listVirtualMachines()["virtualmachine"]
                          if vm["id"] in ids)
    return ips


def list_ips(args):
    print("Listing IP addresses...")
    print(get_nodes(args))


def ssh(args):
    ip = list(get_nodes(args).values())[args.n]
    os.system("{} {}@{}".format(SSH_CMD, SSH_USERNAME, ip))


def install_node(k, nodes, hosts, pub_key):
    sess = create_ssh_session(nodes[k])
    channel = sess.invoke_shell()
    stdin = channel.makefile("wb")
    stdout = channel.makefile("rb")
    stderr = channel.makefile_stderr("rb")
    stdin.write("echo '{}' >> ~/.ssh/authorized_keys\n".format(pub_key))
    stdin.write("echo -e '{}' | sudo tee --append /etc/hosts\n".format(hosts))
    if args.rain_binary:
        os.system("scp {} {}@{}:~/rain ".format(args.rain_bin, SSH_USERNAME, nodes[k]))
        stdin.write("sudo mv ~/rain /usr/local/bin/rain\n")
    if args.rain_wheel:
        rain_whl = os.path.basename(args.rain_wheel)
        os.system("scp {} {}@{}:~/{}".format(args.rain_wheel, SSH_USERNAME, nodes[k], rain_whl))
        stdin.write("pip3 install ~/{}\n".format(rain_whl))
    if args.rain_download:
        url_base = "https://github.com/substantic/rain/releases/download"
        nightly = ""
        if ".dev" in args.rain_download:
            nightly = "nightly-"
        bin_url = ("{}/{}v{}/rain-v{}-linux-x64.tar.xz"
                   .format(url_base, nightly, args.rain_download, args.rain_download))
        stdin.write("wget -O ~/rain.tar.xz {}\n".format(bin_url))
        stdin.write("tar xf ~/rain.tar.xz\n")
        stdin.write("sudo mv ./rain-v{}-linux-x64/rain /usr/local/bin/\n"
                    .format(args.rain_download))

        rain_whl = "rain-{}-py3-none-any.whl".format(args.rain_download)
        python_url = ("{}/{}v{}/{}"
                      .format(url_base, nightly, args.rain_download, rain_whl))
        stdin.write("wget {}\n".format(python_url))
        stdin.write("pip3 install ~/{}\n".format(rain_whl))
    stdin.write("echo -e '{}' > ~/node-list\n".format("\n".join(nodes)))
    stdin.write("exit\n")
    print(stderr.read())
    print(stdout.read().decode("utf-8"))
    stdout.close()
    stdin.close()
    sess.close()


def install(args):
    nodes = get_nodes(args)
    server_ip = list(nodes.values())[0]
    os.popen("{} {}@{} \"ssh-keygen -t rsa -N '' -f ~/.ssh/id_rsa\""
             .format(SSH_CMD, SSH_USERNAME, server_ip)).read()
    pub_key = os.popen("{} {}@{} \"cat ~/.ssh/id_rsa.pub\""
                       .format(SSH_CMD, SSH_USERNAME, server_ip)).read().rstrip()
    hosts = "\n".join(["{} {}".format(nodes[k], k) for k in nodes.keys()])
    processes = []
    for k in nodes.keys():
        p = multiprocessing.Process(target=install_node, args=(k, nodes, hosts, pub_key))
        p.start()
        processes.append(p)
    [p.join() for p in processes]


def start(args):
    nodes = get_nodes(args)
    server_ip = list(nodes.values())[0]
    os.popen("{} {}@{} \"rain start --governor-host-file ~/node-list\""
             .format(SSH_CMD, SSH_USERNAME, server_ip))
    print("Server IP: {}".format(list(nodes.values())[0]))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help="sub-command help")

    parser_create = subparsers.add_parser("create", help="create help")
    parser_create.add_argument("-n", help="Number of virtual machines", type=int, default=1)
    parser_create.add_argument("--name", help="Virtual machine name prefix", default="default")
    parser_create.add_argument("--env", help="Path to environment file")
    parser_create.add_argument("--keypair", choices=SSH_KEY_PAIRS,
                               help="SSH key name", required=True)
    parser_create.add_argument("--offering", choices=OFFERINGS.keys(), default="small",
                               help="Service offering")
    parser_create.add_argument("--zone", choices=ZONES.keys(), default="at-vie-1", help="Zone")
    parser_create.set_defaults(func=create)

    parser_destroy = subparsers.add_parser("destroy", help="destroy help")
    parser_destroy.add_argument("--env", help="Path to .env file", required=True)
    parser_destroy.set_defaults(func=destroy)

    parser_ips = subparsers.add_parser("list-nodes", help="list nodes")
    parser_ips.add_argument("--env", help="Path to .env file", required=True)
    parser_ips.set_defaults(func=list_ips)

    parser_ssh = subparsers.add_parser("ssh", help="SSH into n-th node")
    parser_ssh.add_argument("n", help="Node index", type=int)
    parser_ssh.add_argument("--env", help="path to .env file", required=True)
    parser_ssh.set_defaults(func=ssh)

    parser_install = subparsers.add_parser("install", help="install help")
    parser_install.add_argument("--env", help="path to .env file", required=True)
    parser_install.add_argument("--rain-download", help="rain release version")
    parser_install.add_argument("--rain-binary", help="path to Rain binary")
    parser_install.add_argument("--rain-wheel", help="path to Rain Python wheel")
    parser_install.set_defaults(func=install)

    parser_start = subparsers.add_parser("start", help="start help")
    parser_start.add_argument("--env", help="path to .env file", required=True)
    parser_start.add_argument("-S", help="passes -S to rain start command")
    parser_start.set_defaults(func=start)

    args = parser.parse_args()
    args.func(args)
