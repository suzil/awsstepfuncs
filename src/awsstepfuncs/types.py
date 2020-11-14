from typing import Any, Callable, Dict

ResourceToMockFn = Dict[str, Callable[[Dict[str, Any], Dict[str, Any]], Any]]
