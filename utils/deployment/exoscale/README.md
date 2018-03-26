# Exoscale Rain Deployment Tool

This tool enables to simply create, deploy, manage and destroy computational resources at [Exoscale](https://www.exoscale.com/) cloud infrastructure.

## Usage

The tool is based on [Python Cloustack API client](https://github.com/exoscale/cs) and requires the following configuration in either `$HOME/.cloudstack.ini` or `./cloudstack.ini`:

```
[cloudstack]
endpoint = https://api.exoscale.ch/compute
key = cloudstack api key
secret = cloudstack api secret
# Optional ca authority certificate
verify = /path/to/certs/exoscale_ca.crt
# Optional client PEM certificate
cert = /path/to/client_exoscale.pem
```

### Create a set of virtual machines (environment)

`python3 exoscale.py create --help`

```
usage: exoscale.py create [-h] [--n N] [--name NAME] [--env ENV] --keypair
                          {key1,key2}
                          [--offering {micro,tiny,small,medium,large,extra-large,huge,mega,titan,gpu-small,gpu-huge}]
                          [--zone {ch-gva-2,ch-dk-2,at-vie-1,de-fra-1}]
                          [--rain RAIN]

optional arguments:
  -h, --help            show this help message and exit
  --n N                 Number of virtual machines
  --name NAME           Virtual machine name prefix
  --env ENV             Path to environment file
  --keypair {key1,key2}
                        SSH key name
  --offering {micro,tiny,small,medium,large,extra-large,huge,mega,titan,gpu-small,gpu-huge}
                        Service offering
  --zone {ch-gva-2,ch-dk-2,at-vie-1,de-fra-1}
                        Zone
  --rain RAIN           Rain version
```

All VMs created will have Rain binary located in `/usr/local/bin/rain`.

### Destroy an environment

`python3 exoscale.py destroy --help`

```
usage: exoscale.py destroy [-h] --env ENV

optional arguments:
  -h, --help  show this help message and exit
  --env ENV   Path to .env file
```

### SSH into a VM in an environment

`python3 exoscale.py ssh --help`

```
usage: exoscale.py ssh [-h] --env ENV n

positional arguments:
  n           Node index

optional arguments:
  -h, --help  show this help message and exit
  --env ENV   Path to .env file
```

### List environment IP Adresses

`python3 exoscale.py list-ips --help`

```
usage: exoscale.py list-ips [-h] --env ENV

optional arguments:
  -h, --help  show this help message and exit
  --env ENV   Path to .env file
```

## Example

To spawn an environment with two (`--n`) tiny (default value for `--offering`) VMs, both accessible using `mykey` keypair (`--keypair`) run `python3 exoscale.py create --n 2 --name test --keypair mykey`. This will also create a local environment file `./test.env` which is then used as a reference to the created environment using `--env` switch. Each of the created VMs can be easily SSH-ed into using `python3 exoscale.py ssh 0 --env test.env` or `python3 exoscale.py ssh 1 --env test.env`, respectively. All of the VMs have Rain binary available at `/usr/local/bin/rain`. To list IP adresses of all VMs within the environment use `python3 exoscale.py list-ips --env test.env`. To destroy all VMs within the environment including the environment file run `python3 exoscale.py destroy --env test.env`.