import capnp
import os
import shutil


def remove_dir_content(path):
    """Remove content of the directory but not the directory itself"""
    for item in os.listdir(path):
        p = os.path.join(path, item)
        if os.path.isdir(p):
            shutil.rmtree(p)
        else:
            os.unlink(p)


def load_capnp(filename):
    src_dir = os.path.dirname(__file__)
    capnp.remove_import_hook()
    return capnp.load(os.path.join(src_dir, "../capnp", filename))
