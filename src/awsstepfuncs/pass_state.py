from awsstepfuncs.state_machine import State


class PassState(State):
    """Pass state.

    A Pass state passes its input to its outputs without performing work.
    """

    pass
