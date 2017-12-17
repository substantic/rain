import capnp
import os

SRC_DIR = os.path.dirname(__file__)
capnp.remove_import_hook()
common = capnp.load(SRC_DIR + "/../../../capnp/common.capnp")
server = capnp.load(SRC_DIR + "/../../../capnp/server.capnp")