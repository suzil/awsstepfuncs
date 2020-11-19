# Unit tests

Unit tests in this directory should have full coverage. There are doctests with less coverage, but they should only be redundant tests.

The reason for having full coverage for unit tests and not relying on doctests is that doctests are difficult to debug due to the lack of ability to use a debugger, and additionally it becomes unclear which tests are covered by unit tests and which are covered by doctests.
