import capnp
import os
import shutil


def remove_dir_content(path):
    """Remove content of the directory but not the directory itself"""
    for item in os.listdir(path):
        path = os.path.join(path, item)
        if os.path.isfile(path):
            os.unlink(path)
        else:
            shutil.rmtree(path)


def load_capnp(filename):
    src_dir = os.path.dirname(__file__)
    capnp.remove_import_hook()
    return capnp.load(os.path.join(src_dir, "../capnp", filename))