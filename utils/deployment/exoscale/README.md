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

### Install Rain

`python3 exoscale.py install --help`

### Start Rain

`python3 exoscale.py install --help`

### Destroy an environment

`python3 exoscale.py destroy --help`

### SSH into a VM in an environment

`python3 exoscale.py ssh --help`

### List environment IP Adresses

`python3 exoscale.py list-ips --help`

## Example

```
python3 exoscale.py create -n 2 --keypair <KEYPAIR-NAME>
python3 exoscale.py install --env default.env --rain-download 0.2.2-pre
python3 exoscale.py start --env default.env
python3 exoscale.py destroy --env default.env
```
