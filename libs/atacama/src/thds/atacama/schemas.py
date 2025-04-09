import typing as ty

import marshmallow  # type: ignore
from typing_extensions import Protocol

from ._attrs import Attribute, generate_attrs_post_load, is_attrs_class, yield_attributes
from ._cache import GenSchemaCachingDeco
from ._config import PerGenerationConfigContext, _GenConfig
from ._meta import SchemaMeta
from .field_transforms import FieldTransform, apply_field_xfs
from .fields import generate_field
from .leaf import AtacamaBaseLeafTypeMapping, LeafTypeMapping


def _set_default(default: object = marshmallow.missing) -> ty.Dict[str, ty.Any]:
    """Generate the appropriate Marshmallow keyword arguments depending on whether the default is missing or not"""
    config = PerGenerationConfigContext()
    if default is not marshmallow.missing:
        # we have a default
        field_kwargs = dict(dump_default=default, load_default=default)
        if config.require_all:
            field_kwargs.pop("load_default")  # can't combine load_default with required
            field_kwargs["required"] = True
        return field_kwargs
    return dict(required=True)


def _is_schema(a: ty.Any):
    return isinstance(a, marshmallow.Schema) or (
        isinstance(a, type) and issubclass(a, marshmallow.Schema)
    )


class NamedFieldsSchemaGenerator(Protocol):
    def __call__(self, __attrs_class: type, **__fields: "NamedField") -> ty.Type[marshmallow.Schema]:
        ...  # pragma: nocover


class _NestedSchemaGenerator:
    def __init__(
        self,
        sg: NamedFieldsSchemaGenerator,
        field_kwargs: ty.Mapping[str, ty.Any],
        fields: "ty.Mapping[str, NamedField]",
    ):
        self._schema_generator = sg
        self.field_kwargs = field_kwargs
        self._fields = fields
        # to be used by the discriminator

    def __call__(self, typ: type) -> ty.Type[marshmallow.Schema]:
        return self._schema_generator(typ, **self._fields)


class _PartialField(ty.NamedTuple):
    field_kwargs: ty.Mapping[str, ty.Any]


NamedField = ty.Union[
    marshmallow.fields.Field,
    _NestedSchemaGenerator,
    _PartialField,
    ty.Type[marshmallow.Schema],
    marshmallow.Schema,
]


class SchemaGenerator:
    """A Marshmallow Schema Generator.

    Recursively generates Schemas and their Fields from attrs classes
    and their attributes, allowing selective overrides at every level
    of the recursive type.

    When we generate a Marshmallow Field, about half of the 'work' is
    something that can logically be derived from the context (e.g.,
    does the field have a default, is it required, is it a list, etc)
    and the other half is specific to the use case (do I want
    additional validators, is it load_only, etc).

    We aim to make it easy to layer in the 'specific' stuff while
    keeping the 'given' stuff from the context, to reduce accidents
    and having to repeat yourself.
    """

    def __init__(
        self,
        meta: SchemaMeta,
        field_transforms: ty.Sequence[FieldTransform],
        *,
        leaf_types: LeafTypeMapping = AtacamaBaseLeafTypeMapping,
        cache: ty.Optional[GenSchemaCachingDeco] = None,
    ):
        self._meta = meta
        self._field_transforms = field_transforms
        self._leaf_types = leaf_types
        if cache:
            self.generate = cache(self.generate)  # type: ignore

    def __call__(
        self,
        __attrs_class: type,
        __config: ty.Optional[_GenConfig] = None,
        **named_fields: NamedField,
    ) -> ty.Type[marshmallow.Schema]:
        """Generate a Schema class from an attrs class.

        High-level convenience API that allows for using keyword arguments.
        """
        return self.generate(__attrs_class, config=__config, fields=named_fields)

    def generate(
        self,
        attrs_class: type,
        *,
        fields: ty.Mapping[str, NamedField] = dict(),  # noqa: B006
        config: ty.Optional[_GenConfig] = None,
        schema_base_classes: ty.Tuple[ty.Type[marshmallow.Schema], ...] = (marshmallow.Schema,),
    ) -> ty.Type[marshmallow.Schema]:
        """Low-level API allowing for future keyword arguments that do not overlap with NamedFields.

        May include caching if the SchemaGenerator is so-equipped.
        """
        assert is_attrs_class(attrs_class), (
            f"Object {attrs_class} (of type {type(attrs_class)}) is not an attrs class. "
            "If this has been entered recursively, it's likely that you need a custom leaf type definition."
        )
        config = config or PerGenerationConfigContext()
        with PerGenerationConfigContext.set(config):
            return type(
                ".".join((attrs_class.__module__, attrs_class.__name__))
                + f"{config.schema_name_suffix}Schema",
                schema_base_classes,
                dict(
                    apply_field_xfs(
                        self._field_transforms,
                        self._gen_fields(attrs_class, **fields),
                    ),
                    Meta=self._meta,
                    __atacama_post_load=generate_attrs_post_load(attrs_class),
                    __generated_by_atacama=True,  # not used for anything currently
                ),
            )

    def _named_field_discriminator(
        self, attribute: Attribute, named_field: NamedField
    ) -> marshmallow.fields.Field:
        """When we are given a field name with a provided value, there are 4 possibilities."""
        # 1. A Field. This should be plugged directly into the Schema
        # without being touched.  Recursion ends here.
        if isinstance(named_field, marshmallow.fields.Field):
            return named_field
        # 2. A Schema. You may already have generated (or defined) a
        # Schema for your type. In this case, we simply want to create
        # a Nested field for you with the appropriate outer keyword
        # arguments for the field, since we know whether this is
        # optional, required, etc. Recursion will end as soon as the
        # parts of the type that affect the field keyword arguments
        # for Nested have been stripped and then applied to the NestedField.
        if _is_schema(named_field):
            return generate_field(
                self._leaf_types,
                lambda _s: ty.cast(ty.Type[marshmallow.Schema], named_field),
                attribute.type,
                _set_default(attribute.default),
                debug_name=attribute.name,
            )
        # 3. A nested Schema Generator, with inner keyword
        # arguments. This would be used in the case where you want the
        # outer keyword arguments for Nested to be generated by the
        # current generator, and the nested Schema itself generated
        # from the type, but you want to change the SchemaGenerator
        # context (either the Meta or the field_transforms). Recursion
        # will continue inside the new Schema Generator
        # provided. These are created by passing a SchemaGenerator to
        # .nested on the current SchemaGenerator.
        if isinstance(named_field, _NestedSchemaGenerator):
            return generate_field(
                self._leaf_types,
                named_field,
                attribute.type,
                named_field.field_kwargs,
                debug_name=attribute.name,
            )
        # 4. A partial Field with some 'inner' keyword arguments for
        # the Field only. Recursion continues - simply adds keyword
        # arguments to the Field being generated.
        assert isinstance(named_field, _PartialField), (
            "Named fields must be a Field or Schema, "
            "or must be created with `.field` or `.nested` on a SchemaGenerator. Got: "
            + str(named_field)
        )
        return generate_field(
            self._leaf_types,
            self,
            attribute.type,
            dict(_set_default(attribute.default), **named_field.field_kwargs),
            debug_name=attribute.name,
        )

    def _gen_fields(
        self, __attrs_class: type, **named_fields: NamedField
    ) -> ty.Dict[str, marshmallow.fields.Field]:
        """Internal helper for iterating over attrs fields and generating Marshmallow fields for each"""
        names_onto_fields = {
            attribute.name: (
                generate_field(
                    self._leaf_types,
                    self,
                    attribute.type,
                    _set_default(attribute.default),
                )
                if attribute.name not in named_fields
                else self._named_field_discriminator(attribute, named_fields.pop(attribute.name))
            )
            for attribute in yield_attributes(__attrs_class)
            if attribute.init
        }
        if named_fields:
            # This is just here to avoid people debugging mysterious issues
            raise KeyError(
                f"Named attribute(s) {named_fields.keys()} not found "
                f"in the `attrs` class {__attrs_class.__class__.__name__} "
                " - this indicates incorrect (possibly misspelled?) keyword argument(s)."
            )
        return names_onto_fields

    def field(self, **inner_field_kwargs) -> _PartialField:
        """Defines a field within the context of an existing Schema and attrs type."""
        return _PartialField(inner_field_kwargs)

    def nested(self, **outer_field_kwargs) -> ty.Callable[..., _NestedSchemaGenerator]:
        def make_nsg(**fields) -> _NestedSchemaGenerator:
            return _NestedSchemaGenerator(self, outer_field_kwargs, fields)

        return make_nsg
