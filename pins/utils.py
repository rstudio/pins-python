import sys

from functools import update_wrapper
from types import MethodType

from .config import pins_options


def inform(log, msg):
    if log is not None:
        log.info(msg)

    if not pins_options.quiet:
        print(msg, file=sys.stderr)


class ExtendMethodDoc:
    # Note that the indentation assumes these are top-level method docstrings,
    # so are indented 8 spaces (after the initial sentence).
    template = """\
{current_doc}

        Parent method documentation:

        {parent_doc}
        """

    def __init__(self, func):
        self.func = func

        # allows sphinx to add the method signature to the docs
        # this is pretty benign, since it's very hard to call a descriptor
        # after class initialization (where __set_name__ is called).
        self.__call__ = func

    def __set_name__(self, owner, name):
        bound_parent_meth = getattr(super(owner, owner), name)

        self._parent_doc = bound_parent_meth.__doc__
        self._orig_doc = self.func.__doc__

        if self._orig_doc is not None:
            # update the docstring of the subclass method to include parent doc.
            self.func.__doc__ = self.template.format(
                current_doc=self._orig_doc, parent_doc=self._parent_doc
            )

        # make descriptor look like wrapped function
        update_wrapper(
            self, self.func, ("__doc__", "__name__", "__module__", "__qualname__")
        )

    def __get__(self, obj, objtype=None):
        if obj is None:
            # accessing from class, return descriptor itself.
            return self

        # accessing from instance
        return MethodType(self.func, obj)

    def __call__(self, *args, **kwargs):
        # this is defined, so that callable(ExtendMethodDoc(...)) is True,
        # which allows all the inspect machinery to give sphinx the __call__
        # attribute we set in __init__.
        raise NotImplementedError()
