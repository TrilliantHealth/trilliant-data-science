# currently only in use by the _attrs type introspection tool,
# but probably could be useful for other things, and doesn't directly deal with attrs.
import typing as ty


def _resolve_typevars(
    type_hint: type, type_params: ty.Tuple[ty.TypeVar, ...], type_args: ty.Tuple[type, ...]
) -> type:
    """Recursively resolve TypeVars in a type hint with their actual types."""

    # Direct TypeVar replacement - this is the 'base case'
    if isinstance(type_hint, ty.TypeVar) and type_hint in type_params:
        idx = type_params.index(type_hint)
        if idx < len(type_args):
            return type_args[idx]
        return type_hint

    # Handle generic types that might contain TypeVars
    if hasattr(type_hint, "__origin__"):
        origin = type_hint.__origin__
        args = getattr(type_hint, "__args__", ())

        # Recursively resolve each argument
        resolved_args = tuple(_resolve_typevars(arg, type_params, type_args) for arg in args)

        # If nothing changed, return the original
        if resolved_args == args:
            return type_hint

        # Create a new generic with resolved arguments
        try:
            return origin[resolved_args]
        except (TypeError, IndexError):
            # Fall back to original if we can't create a new generic
            return type_hint

    # Return unmodified for simple types
    return type_hint


def base_class_and_hints(class_: type) -> ty.Tuple[type, ty.Dict[str, type]]:
    # Get the original class and type arguments if it's a generic
    if not hasattr(class_, "__origin__"):
        return class_, ty.get_type_hints(class_)

    # Get type hints from the base class
    base_class = class_.__origin__
    base_hints = ty.get_type_hints(base_class)
    if not hasattr(base_class, "__parameters__"):
        return base_class, base_hints

    # Create a modified hints dictionary with resolved types
    type_params = base_class.__parameters__
    assert hasattr(
        class_, "__args__"
    ), f"Generic class {class_} must have type arguments, or we cannot determine types for it."
    type_args = class_.__args__
    hints = {}

    for name, hint_type in base_hints.items():
        # Check if this hint is a TypeVar or contains TypeVars
        resolved_type = _resolve_typevars(hint_type, type_params, type_args)
        hints[name] = resolved_type

    return base_class, hints
