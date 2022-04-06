class PinsError(Exception):
    pass


class PinsVersionError(PinsError):
    pass


class PinsInsecureReadError(PinsError):
    pass
