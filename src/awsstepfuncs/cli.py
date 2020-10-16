"""Comand-line interface (CLI) for AWS Step Functions SDK."""


def cli() -> str:
    """Import and return the current version of this package.

    Returns:
        The current version
    """
    from awsstepfuncs import __version__

    return __version__
