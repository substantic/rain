import json
from cs import CloudStack, read_config
import argparse
import os
import base64


def print_pretty(json_data):
    print(json.dumps(json_data, indent=4, sort_keys=True))


cs = CloudStack(**read_config())

OFFERINGS = {o["name"].lower(): o["id"] for o in cs.listServiceOfferings()["serviceoffering"]}
ZONES = {z["name"].lower(): z["id"] for z in cs.listZones()["zone"]}
SSH_KEY_PAIRS = [kp["name"] for kp in cs.listSSHKeyPairs()["sshkeypair"]]

RAIN_VERSION = "v0.1.1"
VIRTUAL_MACHINES = cs.listVirtualMachines()
TEMPLATE_OFFERINGS = cs.listTemplates(templatefilter="executable")
NETWORK_OFFERINGS = cs.listNetworkOfferings()
SECURITY_GROUPS = cs.listSecurityGroups()
INIT_SCRIPT_TEMPLATE = """
#cloud-config
runcmd:
    - cd /tmp
    - wget https://github.com/substantic/rain/releases/download/{version}/rain-{version}-linux-x64.tar.xz
    - tar xf rain-{version}-linux-x64.tar.xz
    - cp /tmp/rain-{version}-linux-x64/rain /usr/local/bin/
""" # noqa


def generate_init_script(version):
    return INIT_SCRIPT_TEMPLATE.format(version=version)


def create(args):
    print("Creating...")
    vms = []
    for i in range(args.n):
        vms.append(cs.deployVirtualMachine(
            name="{}-{}".format(args.name, i),
            serviceOfferingId=OFFERINGS[args.offering],
            templateId="4fedad2b-e96c-4a70-95f7-a9142995dba4",
            zoneId=ZONES[args.zone],
            keypair=args.keypair,
            userdata=base64.b64encode(generate_init_script(args.rain).encode("utf-8"))))
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


def get_ips(args):
    ips = []
    with open(args.env, "r") as f:
        ids = [vm["id"] for vm in json.loads(f.read())]
        ips = [vm["nic"][0]["ipaddress"]
               for vm in cs.listVirtualMachines()["virtualmachine"]
               if vm["id"] in ids]
    return ips


def list_ips(args):
    print("Listing IP addresses...")
    print(get_ips(args))


def ssh(args):
    ip = get_ips(args)[args.n]
    user = "ubuntu"
    os.system("ssh {}@{}".format(user, ip))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help="sub-command help")

    parser_create = subparsers.add_parser("create", help="create help")
    parser_create.add_argument("--n", help="Number of virtual machines", type=int, default=1)
    parser_create.add_argument("--name", help="Virtual machine name prefix")
    parser_create.add_argument("--env", help="Path to environment file")
    parser_create.add_argument("--keypair", choices=SSH_KEY_PAIRS,
                               help="SSH key name", required=True)
    parser_create.add_argument("--offering", choices=OFFERINGS.keys(), default="tiny",
                               help="Service offering")
    parser_create.add_argument("--zone", choices=ZONES.keys(), default="at-vie-1", help="Zone")
    parser_create.add_argument("--rain", default="v0.1.1", help="Rain version")
    parser_create.set_defaults(func=create)

    parser_destroy = subparsers.add_parser("destroy", help="destroy help")
    parser_destroy.add_argument("--env", help="Path to .env file", required=True)
    parser_destroy.set_defaults(func=destroy)

    parser_ips = subparsers.add_parser("list-ips", help="List IP addresses")
    parser_ips.add_argument("--env", help="Path to .env file", required=True)
    parser_ips.set_defaults(func=list_ips)

    parser_ssh = subparsers.add_parser("ssh", help="SSH into n-th node")
    parser_ssh.add_argument("n", help="Node index", type=int)
    parser_ssh.add_argument("--env", help="Path to .env file", required=True)
    parser_ssh.set_defaults(func=ssh)

    args = parser.parse_args()
    args.func(args)
