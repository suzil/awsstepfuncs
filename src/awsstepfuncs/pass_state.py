from typing import Any, Optional

from awsstepfuncs.state import AbstractState, StateType


class PassState(AbstractState):
    """Pass state in Amazon States Language.

    A Pass state passes its input to its outputs without performing work.
    """

    state_type = StateType.PASS

    def __init__(
        self,
        name: str,
        /,
        *,
        comment: Optional[str] = None,
        input_path: str = "$",
        output_path: str = "$",
        result_path: Optional[str] = "$",
        result: Optional[Any] = None,
    ):
        """Initialize a task state.

        Args:
            name: The name of the state.
            comment: A human-readable description of the state.
            input_path: Used to select a portion of the state input. Default is
                $ (pass everything).
            output_path: Used to select a portion of the state output. Default
                is $ (pass everything).
            result_path: Specifies where (in the input) to place the "output" of
                the virtual task specified in Result. The input is further filtered
                as specified by the OutputPath field (if present) before being used
                as the state's output. Default is $ (pass only the output state).
            result: Treated as the output of a virtual task to be passed to the
                next state, and filtered as specified by the ResultPath field (if
                present).
        """
        self.result = result
        super().__init__(
            name,
            comment=comment,
            input_path=input_path,
            output_path=output_path,
            result_path=result_path,
        )

    def run(self, state_input: dict) -> dict:
        """Execute the pass state according to Amazon States Language.

        Args:
            state_input: The input state data. For the pass state, it will
                simply be passed as the output state with no transformation.

        Returns:
            The output state, same as the input state in this case.
        """
        print("Passing")  # noqa: T001
        if result := self.result:
            return result
        else:
            return state_input
