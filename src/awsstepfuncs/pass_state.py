from awsstepfuncs.state_machine import State, StateType


class PassState(State):
    """Pass state in Amazon States Language.

    A Pass state passes its input to its outputs without performing work.
    """

    state_type = StateType.PASS
