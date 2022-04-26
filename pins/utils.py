import sys

from .config import pins_options


def inform(log, msg):
    if log is not None:
        log.info(msg)

    if not pins_options.quiet:
        print(msg, file=sys.stderr)


class ExtendMethodDoc:
    template = """\
{current_doc}

Parent method documentation:

{parent_doc}
"""

    def __init__(self, func):
        self.func = func

    def __set_name__(self, owner, name):
        bound_parent_meth = getattr(super(owner, owner), name)

        self._parent_doc = bound_parent_meth.__doc__
        self._orig_doc = self.func.__doc__

        self.func.__doc__ = self.template.format(
            current_doc=self._orig_doc, parent_doc=self._parent_doc
        )
        self.__doc__ = self.func.__doc__

    def __get__(self, obj, objtype=None):
        if obj is None:
            return self

        return self.func
