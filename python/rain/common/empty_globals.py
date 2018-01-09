"""
This is a buiding stone for `horror_pickle`, do not touch, do not feed after
midnight. Isolated into this module to limit the globals affected.
"""

import contextlib


@contextlib.contextmanager
def empty_globals():
    """
    Context manager that runs the contained block with almost empty globals.

    Only `__*__`, this function and `contextlib` are left. The globals are restored
    at exit and exceptions. Any modification of the globals inside the block
    are forgotten. The original stored globals are yielded as the manager.

    >>> with empty_globals() as g:
    >>>     assert len(globals()) == 1
    >>>     assert '__name__' in g
    """

    g = dict(globals())
    for n in g:
        if not n.startswith('__'):
            del globals()[n]
    globals()['contextlib'] = g['contextlib']
    globals()['empty_globals'] = g['empty_globals']
    try:
        yield g
    except:  # noqa
        globals().clear()
        globals().update(g)
        raise
    finally:
        globals().clear()
        globals().update(g)
