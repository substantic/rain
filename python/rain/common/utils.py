import cloudpickle
import pickle


_limited_pickle_code = compile("pickle.dumps(obj, protocol=p)",
                               "utils.py",
                               "eval")


def limited_pickle(obj, protocol=None):
    """
    pickle.dumps limited to builtin types: no global objects are allowed.
    """
    loc = {'pickle': pickle, 'obj': obj, 'p': protocol}
    return eval(_limited_pickle_code, {}, loc)


def clever_pickle(obj, protocol=None):
    """
    Pickle `obj` with `pickle` if possible without global symbols, then with `cloudpickle`.

    If `obj` contains no functions, class types, modules or labdas, `pickle.dumps`
    quickly serializes it. Global function/class symbols are disabled by temporarily
    clearing `globals()`. If `pickle` fails, `cloudpickle` is used instead.
    """
    import pickle  # noqa
    try:
        return limited_pickle(obj, protocol=protocol)
    except pickle.PicklingError:
        return cloudpickle.dumps(obj, protocol=protocol)
    except AttributeError:
        return cloudpickle.dumps(obj, protocol=protocol)


def format_size(size_bytes):
    """
    Format size in bytes approximately as kB/MB/GB/...

    >>> format_size(2094521)
    2.1 MB
    """
    if size_bytes < 1e3:
        return "{} B".format(size_bytes)
    elif size_bytes < 1e6:
        return "{:.1} kB".format(size_bytes / 1e3)
    elif size_bytes < 1e9:
        return "{:.1} MB".format(size_bytes / 1e6)
    elif size_bytes < 1e12:
        return "{:.1} GB".format(size_bytes / 1e9)
    else:
        return "{:.1} TB".format(size_bytes / 1e12)
