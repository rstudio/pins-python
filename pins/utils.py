import sys

from .config import pins_options


def inform(log, msg):
    if log is not None:
        log.info(msg)

    if not pins_options.quiet:
        print(msg, file=sys.stderr)
