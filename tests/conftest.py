from contextlib import closing, redirect_stdout
from io import StringIO

import pytest


@pytest.fixture()
def capture_stdout():
    def _capture_stdout(simulate_fn):
        with closing(StringIO()) as fp:
            with redirect_stdout(fp):
                simulate_fn()
            return fp.getvalue()

    return _capture_stdout
