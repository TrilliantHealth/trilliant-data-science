import typing as ty
from enum import Enum

import attrs
import marshmallow as ma
import pytest
from marshmallow_enum import EnumField

import core.atacama as atacama

"""The following is taken more-or-less directly from the README"""


class GooeyEnum(Enum):
    very = "VERY"
    fairly = "FAIRLY"
    somewhat = "SOMEWHAT"
    not_at_all = "NOT_AT_ALL"


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
    milk: ty.Optional[Milk] = None  # definition of Milk omitted for brevity...


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


def test_load_and_dump_smore():
    smore = SmoreFromApiSchema().load(
        dict(
            graham_cracker=dict(brand="Kroger"),
            marshmallows=[
                dict(gooeyness="VERY", color="golden-brown"),
                dict(gooeyness="FAIRLY", color="light-brown"),
            ],
            chocolate=dict(
                cacao_pct=55.0, brand="nestle", ingredients=dict(cacao_src="Ivory Coast", sugar_grams=3)
            ),
        )
    )

    assert smore.chocolate.cacao_pct == 55.0
    assert smore.chocolate.ingredients.cacao_src == "Ivory Coast"
    assert smore.marshmallows[0].gooeyness == GooeyEnum.very

    with pytest.raises(ma.ValidationError):
        ChocolateIngredientsFromApiSchema().load(dict(sugar_grams=3, cacao_src="USA"))
    with pytest.raises(ma.ValidationError):
        ChocolateIngredientsFromApiSchema().load(dict())

    almost = dict(
        graham_cracker=dict(brand="Publix"),
        marshmallows=[dict(gooeyness="VERY")],
        chocolate=dict(brand="nestle", cacao_pct=110.0),  # bad percentage
    )
    with pytest.raises(ma.ValidationError):
        SmoreFromApiSchema().load(almost)

    almost["chocolate"]["cacao_pct"] = 100  # type: ignore
    SmoreFromApiSchema().load(almost)


def test_load_and_dump_fully_generated():
    smore_schema = atacama.neo(Smore)

    with pytest.raises(ma.ValidationError):
        smore_schema().load(
            dict(graham_cracker=dict(brand="Publix"), marshmallows=list())
        )  # can't have 0 marshmallows

    smore_schema().load(
        dict(graham_cracker=dict(brand="Publix"), marshmallows=[dict(gooeyness="fairly")])
    )
