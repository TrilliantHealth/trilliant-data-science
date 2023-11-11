# atacama

A Marshmallow schema generator for `attrs` classes.

Inspired by `desert`.

## Why

`desert` seems mostly unmaintained. It is also surprisingly small (kudos to the authors), which makes it
a reasonable target for forking and maintaining.

However, we think the (widespread) practice of complecting the data class definition with its
serialization schema is unwise. While this is certainly DRY-er than having to rewrite the entire Schema,
it's (critically) not DRY at all if you ever want to have different de/serialization patterns depending
on the data source.

In particular, `atacama` is attempting to optimize for the space of Python application that serve APIs
from a database. These are common situations where serialization and deserialization may need to act
differently, and there's value in being able to cleanly separate those without redefining the `attrs`
class itself.

`cattrs` is the prior art here, which mostly dynamically defines all of its structure and unstructure
operations, and allows for different Converters to be used on the same `attrs` classes. However `cattrs`
does not bring the same level of usability as Marshmallow when it comes to various things that are
important for APIs. In particular, we prefer Marshmallow for its:

- validation, which we find to be more ergonomic in the Marshmallow-verse.
- ecosystem utilities such as OpenAPI spec generation from Marshmallow Schemas.

As of this writing, we are unaware of anything that `cattrs` can do that we cannot accomplish in
Marshmallow, although for performance and other reasons, there may be cases where `cattrs` remains a
better fit!

Thus `atacama`. It aims to provide fully dynamic Schema generation, while retaining 100% of the
generality offered by Marshmallow, in a form that avoids introducing complex shim APIs that no longer
look and feel like Marshmallow itself.

## What

`atacama` takes advantage of Python keyword arguments to provide as low-boilerplate an interface as
possible. Given:

```
from datetime import datetime, date
import attrs


@attrs.define
class Todo:
    id: str
    owner_id: str
    created_at: datetime
    priority: float = 0.0
    due_on: None | date = None
```

For such a simple example, let's assume the following Schema validation rules, but only for when the data
comes in via the API:

- `created_at` must be before the current moment
- `priority` must be in the range \[0.0, 10.0\]
- `due_on`, if present, must be before 2038, when the Unix epoch will roll over and all computers will
  die a fiery death.

```
from typing import Type
from atacama import neo  # neo is the recommended default SchemaGenerator
import marshmallow as ma


def before_now(dt: datetime) -> bool:
    return dt <= datetime.now()


def before_unix_death(date: date):
    return date < date(2038, 1, 19)


TodoFromApi: Type[ma.Schema] = neo(
    Todo,
    created_at=neo.field(validate=before_now),
    priority=neo.field(validate=ma.validate.Range(min=0.0, max=10.0),
    due_on=neo.field(validate=before_unix_death),
)
TodoFromDb: Type[ma.Schema] = neo(
    Todo,
    created_at=neo.field(data_key='created_ts'),
)
# both of the generated Schemas are actually Schema _classes_,
# just like a statically defined Marshmallow class.
# In most cases, you'll want to instantiate an object of the class
# before use, e.g. `TodoFromDb().load(...)`
```

Note that nothing that we have done here requires

- modifying the `Todo` class in any way.
- repeating any information that can be derived _from_ the `Todo` class (e.g. that `due_on` is a `date`,
  or that it is `Optional` with a default of `None`).
- complecting the data source and validation/transformation for that source with the core data type
  itself, which can easily be shared across both the database and the API.

### Recursive Schema and Field generation

The first example demonstrates what we want and why we want it, but does not prove generality for our
approach. Classes are by nature recursively defined, and Schemas must also be.

Happily, `atacama` supports recursive generation and recursive customization at each layer of the
class+`Schema`.

There are five fundamental cases for every attribute in a class which is desired to be a `Field` in a
Schema. Two of these have already been demonstrated. The 5 cases are the following:

1. Completely dynamic `Field` and recursive `Schema` generation.

- This is demonstrated by `id` and `owner_id` in our `Todo` example. We told `atacama` nothing about
  them, and reasonable Marshmallow Fields with correct defaults were generated for both.

2. A customized `Field`, with recursive `Schema` generation as needed.

- This is demonstrated by `created_at`, `priority`, and `due_on` in our `Todo` example. Much information
  can be dynamically derived from the annotations in the `Todo` class, and `atacama` will do so. However,
  we also wished to _add_ information to the generated `Field`, and we can trivially do so by supplying
  keyword arguments normally accepted by `Field` directly to the `field` method of our `SchemaGenerator`.
  These keyword arguments can even technically override the keyword arguments for `Field` derived by
  `atacama` itself, though that would in most cases be a violation of your contract with the readers of
  your class definition and is therefore not recommended. The `Field` _type_ will still be chosen by
  `atacama`, so if for some reason you want more control than is being offered by `atacama`, that takes
  you to option #3:

3. Completely static `Field` definition.

- In some cases, you may wish to opt out of `atacama` entirely, starting at a given attribute. In this
  case, simply provide a Marshmallow `Field` (which is by definition fully defined recursively), and
  `atacama` will respect your intention by placing the `Field` directly into the `Schema` at the
  specified point.

4. A statically defined `Schema`.

- This is similar to case 2, except that, by providing a Marshmallow `Schema` for a nested attribute, you
  are confirming that you want `atacama` to infer the "outer" information about that attribute, including
  that is is a `Nested` `Field`, to perform all the standard unwrapping of Generic and Union types, and
  to assign the correct default based on your `attrs` class definition. For instance, an attribute that
  exhibits the definition `Optional[List[YourClass]] = None` would allow you to provide a nested `Schema`
  defining only how to handle `YourClass`, while still generating the functionality around the default
  value None and expecting a `List` of `YourClass`.
- In particular, this would be an expected case when you have a need to generate a `Schema` for direct
  deserialization of a class that is also used in a parent class and `Schema`, but where both the parent
  and child Schema share all the same custom validation, etc. By generating the nested `Schema` and then
  assigning it at the proper location within the parent `Schema`, you can easily reuse all of the
  customization from the child generation.

5. A nested `Schema` _generator_.

- The most common use case for this will be when it is desirable to customize the generated `Field` of a
  nested class. In order to provide an API that continues to privilege keyword arguments as a way of
  'pathing' to the various parts of the `Schema`, we must first capture any keyword arguments specific to
  the `Nested` `Field` that will be generated, and from there on we can allow you to provide names
  pointing to attributes in the nested class.
- SchemaGenerators are objects created by users who wish to customize `Schema` generation in particular
  ways. The `Meta` class within a Marshmallow `Schema` changes certain behaviors across all its fields.
  While `atacama` provides several default generators, you may wish to create your own. Regardless, the
  use case for providing a nested `SchemaGenerator` is more specifically where you wish to make Schemas
  with nested Schemas that follow different rules than their parents. This is no issue with `atacama` -
  if it finds a nested `SchemaGenerator`, it will defer nested generation from that point onward to the
  new `SchemaGenerator` as expected. Note that, of course, the `Field` being generated for that attribute
  will follow the rules of the _current_ SchemaGenerator, just as would happen with nested `Meta` classes
  in nested Schemas.

What does this look like in practice? See the annotated example below, which demonstrates all 5 of these
possible interactions between an `attrs` class and the specific `Schema` desired by our (potentially
somewhat sugar-high) imaginary user:

```python 3.7
@attrs.define
class Mallow:
    gooeyness: GooeyEnum
    color: str = "light-brown"


@attrs.define
class Milk:
    """Just a percentage"""

    fat_pct: float


@attrs.define
class ChocolateIngredients:
    cacao_src: str
    sugar_grams: float
    milk: ty.Optional[Milk] = None


@attrs.define
class Chocolate:
    brand: str
    cacao_pct: float
    ingredients: ty.Optional[ChocolateIngredients] = None


@attrs.define
class GrahamCracker:
    brand: str


@attrs.define
class Smore:
    graham_cracker: GrahamCracker
    marshmallows: ty.List[Mallow]
    chocolate: ty.Optional[Chocolate] = None


ChocolateIngredientsFromApiSchema = atacama.neo(
    ChocolateIngredients,
    # 1. milk and sugar_grams are fully dynamically generated
    # 2. a partially-customized Field inheriting its Field type, default, etc from the attrs class definition
    cacao_src=atacama.neo.field(
        validate=ma.validate.OneOf(["Ivory Coast", "Nigeria", "Ghana", "Cameroon"])
    ),
)


class MallowSchema(ma.Schema):
    """Why are you doing this by hand?"""

    gooeyness = EnumField(GooeyEnum, by_value=True)
    color = ma.fields.Raw()

    @ma.post_load
    def pl(self, data: dict, **_kw):
        return Mallow(**data)


SmoreFromApiSchema = atacama.ordered(
    Smore,
    # 1. graham_cracker, by being omitted, will have a nested schema generated with no customizations
    # 5. In order to name/path the fields of nested elements, we plug in a nested
    # SchemaGenerator.
    #
    # Note that keyword arguments applicable to the Field surrounding the nested Schema,
    # e.g. load_only, are supplied to the `nested` method, whereas 'paths' to attributes within the nested class
    # are supplied to the returned NestedSchemaGenerator function.
    #
    # Note also that we use a different SchemaGenerator (neo) than the parent (ordered),
    # and this is perfectly fine and works as you'd expect.
    chocolate=atacama.neo.nested(load_only=True)(
        # 2. Both pct_cacao and brand have customizations but are otherwise dynamically generated.
        # Note in particular that we do not need to specify the `attrs` class itself, as that
        # is known from the type of the `chocolate` attribute.
        cacao_pct=atacama.neo.field(validate=ma.validate.Range(min=0, max=100)),
        brand=atacama.neo.field(validate=ma.validate.OneOf(["nestle", "hershey"])),
        # 4. we reuse the previously defined ChocolateIngredientsFromApi Schema
        ingredients=ChocolateIngredientsFromApiSchema,
    ),
    # 3. Here, the list of Mallows is represented by a statically defined NestedField
    # containing a statically defined Schema.
    # Why? Who knows, but if you want to do it yourself, it's possible!
    marshmallows=ma.fields.Nested(MallowSchema(many=True)),
)
```

## How

### SchemaGenerators

All interaction with `atacama` is done via a top-level `SchemaGenerator` object. It contains some
contextual information which will be reused recursively throughout a generated `Schema`, including a way
to define the `Meta` class that is a core part of Marshmallow's configurability.

`atacama` currently provides two 'default' schema generators, `neo` and `ordered`.

- `ordered` provides no configuration other than the common specification that the generated Schema
  should preserve the order of the attributes as they appear in the class - while this may not matter for
  most runtime use cases, it is infinitely valuable for debuggability and for further ecosystem usage
  such as OpenAPI spec generation, which ought to follow the order defined by the `attrs` class.

- `neo` stands for "non-empty, ordered", and is the preferred generator for new Schemas, because it
  builds in a very opinionated but nonetheless generally useful concept of non-emptiness. For attributes
  of types that properly have lengths, it is in general the case that one and only one of the following
  should be true:

  1. Your attribute has a default defined, such that it is not required to be present in input data for
     successful deserialization.
  1. It is illegal to provide an empty, zero-length value.

  The intuition here is that a given attribute type either _may_ have an 'essentially empty' value, or it
  may not. Examples of things which may never be empty include database ids (empty string would be
  inappropriate), lists of object 'owners' (an empty list would orphan the object, and therefore must not
  be permitted), etc. Whereas in many cases, an empty string or list is perfectly normal, and in those
  cases it is preferred that the class itself define the common-sense default value in order to make
  things work as expected without boilerplate.

### FieldTransforms

The `neo` `SchemaGenerator` performs the additional 'non-empty' validation to non-defaulted Fields via
something called a `FieldTransform`. Any `FieldTransform` attached to a `SchemaGenerator` will be run on
_every_ `Field` attached to the Schema, _recursively_. This includes statically-provided Fields.

The `FieldTransform` must accept an actual `Field` object and returns a (presumably modified) `Field`
object. This is only run at the time of `Schema` generation, so if you wish to add validators or perform
customization to the Field that happens at load/dump time, you must compose your logic with the existing
`Field`. A Schema generator can have multiple FieldTransforms, and they will be run _in order_ on every
`Field`. A `FieldTransform` is, in essence, a higher-order function over `Field`, which are themselves
functions for the incoming attribute data.

The two default generators are provided as a convenience to the user and nothing more - it is perfectly
acceptable and indeed expected that you might define your own 'sorts' of schema generators, with your own
`FieldTransforms` and basic `Meta` definitions, depending on your needs.

### Leaf type->Field mapping

As a recursive generator, there must be known base cases where a concrete Marshmallow `Field` can be
automatically generated based on the type of an attribute.

#### Built-in mappings

The default base cases are defined in `atacama/leaf.py`. They are relatively comprehensive as far as
Python builtins go, covering various date/time concepts and UUID. We also specifically map
`Union[int, float]` to the Marshmallow `Number` `Field`. Further, we support `typing_extensions.Literal`
using the built-in Marshmallow validator `OneOf`, and we have introduced a simple `Set` `Field` that
serializes `set`s to sorted `list`s.

#### Custom static mappings

Nevertheless, you may find that you wish to configure a more comprehensive (or different) set of leaf
types for your `SchemaGenerator`. This may be configured by passing the keyword argument `leaf_types` to
the `SchemaGenerator` constructor with a mapping of those leaf types. A `dict` is sufficient to provide a
static `LeafTypeMapping`.

#### Custom dynamic mappings

You may also provide a more dynamic implementation of the `Protocol` defined in `atacama/leaf.py`. This
would provide functionality similar to `cattrs.register_structure_hook`, except that a Marshmallow
`Field` handles both serialization and deserialization. The included `DynamicLeafTypeMapping` class can
help accomplish this, though you may provide your own custom implementation of the Protocol as well.
`DynamicLeafTypeMapping` is recursively nestable, so you may overlay your own handlers on top of our base
handlers via:

```
from atacama import DynamicLeafTypeMapping, AtacamaBaseLeafTypeMapping

your_mapping = DynamicLeafTypeMapping(AtacamaBaseLeafTypeMapping, [handler_1, handler_2])
```

## Minor Features

#### `require_all`

You may specify at generation time that you wish to make all fields (recursively) `required` at the time
of load. This may be useful on its own, but is also the only way of accurately describing an 'output'
type in a JSON/OpenAPI schema, because `required` in that context is the only way to indicate that your
attribute will never be `undefined`. When dumping an `attrs` class to Python dictionary, all attributes
are always guaranteed to be present in the output, so `undefined` will never happen even for attributes
with defaults.

Example:

`atacama.neo(Foo, config(require_all=True))`

#### Schema name suffix

You may specify a suffix for the name of the Schema generated. This may be useful when you are trying to
generate an output JSON schema and have multiple Schemas derived from the same `attrs` class.

Example:

`atacama.neo(Foo, config(schema_name_suffix='Input'))` results in the schema having the name
`your_module.FooInput` rather than `your_module.Foo`.
