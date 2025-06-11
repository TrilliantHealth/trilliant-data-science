import os
from unittest import mock

from thds.mops.image import std_find_image_full_tag


def test_get_full_image_ref_from_mops_env():
    with mock.patch.dict(
        os.environ,
        dict(MOPS_IMAGE_FULL_TAG="thdatascience.azurecr.io/ds/fooproj:test1"),
    ):
        assert "thdatascience.azurecr.io/ds/fooproj:test1" == std_find_image_full_tag("fooproj")()


def test_get_nothing_from_version_env_without_any_data():
    assert "" == std_find_image_full_tag()()
