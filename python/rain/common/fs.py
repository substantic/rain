import os
import shutil

import capnp


def remove_dir_content(path):
    """Remove content of the directory but not the directory itself"""
    for item in os.listdir(path):
        p = os.path.join(path, item)
        if os.path.isdir(p):
            shutil.rmtree(p)
        else:
            os.unlink(p)


def fresh_copy_dir(source_path, target_path):
    """Recursively copy directory, the difference between shutil.copy_tree
       is that this function do not copy permission and other metadata"""
    os.mkdir(target_path)
    for item in os.listdir(source_path):
        s = os.path.join(source_path, item)
        t = os.path.join(target_path, item)
        if os.path.isdir(s):
            fresh_copy_dir(s, t)
        else:
            shutil.copyfile(s, t)


def load_capnp(filename):
    src_dir = os.path.dirname(__file__)
    capnp.remove_import_hook()
    return capnp.load(os.path.join(src_dir, "../capnp", filename))
