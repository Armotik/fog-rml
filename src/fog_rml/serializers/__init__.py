from importlib import import_module

NQuadsSerializer = import_module(".NQuadsSerializer", __name__)
NTriplesSerializer = import_module(".NTriplesSerializer", __name__)
TurtleSerializer = import_module(".TurtleSerializer", __name__)

__all__ = ["NQuadsSerializer", "NTriplesSerializer", "TurtleSerializer"]
