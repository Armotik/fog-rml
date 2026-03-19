from .registry import FunctionRegistry

# Load built-in registration and optional plugins
try:
	# builtins registers itself when imported
	from . import builtins  # noqa: F401
except Exception:
	pass

try:
	from . import idlab_plugins  # noqa: F401
except Exception:
	pass

__all__ = ["FunctionRegistry"]
