from enum import Enum, auto
from typing import Any, List, Optional

from colorama import Fore as ColoramaColor
from colorama import Style as ColoramaStyle


class Color(Enum):
    """Wrap Colorama's colors to make typing easier for the user.

    Refs: https://github.com/tartley/colorama/blob/master/colorama/ansi.py#L49 (AnsiFore class)
    """

    BLACK = auto()
    RED = auto()
    GREEN = auto()
    YELLOW = auto()
    BLUE = auto()
    MAGENTA = auto()
    CYAN = auto()
    WHITE = auto()
    LIGHTBLACK_EX = auto()
    LIGHTRED_EX = auto()
    LIGHTGREEN_EX = auto()
    LIGHTYELLOW_EX = auto()
    LIGHTBLUE_EX = auto()
    LIGHTMAGENTA_EX = auto()
    LIGHTCYAN_EX = auto()
    LIGHTWHITE_EX = auto()


class Style(Enum):
    """Wrap Colorama's styles to make typing easier for the user."""

    DIM = auto()
    BRIGHT = auto()


class Printer:
    """Print simulation messages to STDOUT."""

    def __init__(self, colorful: bool = False):
        """Initialize a Printer.

        Args:
            colorful: Whether or not to use colorful STDOUT. Defaults to False.
        """
        self.colorful = colorful

    def __call__(
        self,
        *messages: Any,
        color: Color = None,
        style: Style = None,
        emoji: str = None,
    ) -> None:
        """Print the message to STDOUT with an optional color.

        Args:
            messages: The messages to print.
            color: The color to use.
            style: The style to use.
            emoji: The emoji to use at the beginning.
        """
        to_print = [str(message) for message in messages]
        if self.colorful:  # pragma: no cover
            self._make_colorful(to_print, color=color, style=style, emoji=emoji)

        print(*to_print)

    @staticmethod
    def _make_colorful(
        to_print: List[str],
        /,
        *,
        color: Optional[Color] = None,
        style: Optional[Style] = None,
        emoji: Optional[str] = None,
    ) -> None:  # pragma: no cover
        """Make a colorful print message.

        Args:
            to_print: The list of messages to print which can include color,
                style, and emoji. This reference is mutated in this method.
            color: The color to use.
            style: The style to use.
            emoji: The emoji to use.
        """
        if color or style:
            if color:
                color_value = getattr(ColoramaColor, color.name)
                to_print[0] = color_value + to_print[0]
            if style:
                style_value = getattr(ColoramaStyle, style.name)
                to_print[0] = style_value + to_print[0]
            to_print.append(ColoramaStyle.RESET_ALL)
        if emoji:
            to_print.insert(0, emoji)
