"""Interface for creating a callable from a data type, which accepts a value and validates that the
value is an instance of the type, recursively and respecting generic type parameters"""

from .check import instancecheck, isinstance  # noqa: F401
