class AWSStepFuncsError(Exception):
    """Base error for this package.

    This is useful for client code to know whether the error is expected or
    whether there is a bug in the library code.

    >>> from awsstepfuncs import *

    >>> try:
    ...     wait_state = WaitState("Wait!", seconds=-1)
    ... except AWSStepFuncsError:
    ...     print("Error in the client")
    ... except Exception:
    ...     print("Error in awsstepfuncs")
    Error in the client
    """


class AWSStepFuncsValueError(AWSStepFuncsError):
    """A bad value was specified.

    >>> from awsstepfuncs import *

    >>> wait_state = WaitState("Wait!", seconds=-1)
    Traceback (most recent call last):
        ...
    awsstepfuncs.errors.AWSStepFuncsValueError: seconds must be greater than zero
    """
