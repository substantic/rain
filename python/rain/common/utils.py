
def format_size(size_bytes):
    """
    Format size in bytes approximately as B/kB/MB/GB/...

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


def short_str(s, max_len=32):
    """
    Convert `s` to string and cut it off to at most `maxlen` chars (with an ellipsis).
    """
    if not isinstance(s, str):
        s = str(s)
    if len(s) > max_len:
        s = s[:27] + "[...]"
    return s
