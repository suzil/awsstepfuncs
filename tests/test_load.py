import awsstepfuncs
from awsstepfuncs import cli


def test_load():
    assert awsstepfuncs.__version__ == cli.cli()
