from awsstepfuncs.state import State, StateType


class PassState(State):
    """Pass state in Amazon States Language.

    A Pass state passes its input to its outputs without performing work.
    """

    state_type = StateType.PASS

    def run(self, state_input: dict) -> dict:
        """Execute the pass state according to Amazon States Language.

        Args:
            state_input: The input state data. For the pass state, it will
                simply be passed as the output state with no transformation.

        Returns:
            The output state, same as the input state in this case.
        """
        print("Passing")  # noqa: T001
        return state_input
