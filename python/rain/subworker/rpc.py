import capnp
import os

SRC_DIR = os.path.dirname(__file__)
capnp.remove_import_hook()
subworker = capnp.load(SRC_DIR + "/../../../capnp/subworker.capnp")
