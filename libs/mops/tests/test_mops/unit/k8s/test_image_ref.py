import os
from unittest import mock

from thds.mops.k8s.image_ref import std_find_image_full_tag


def test_get_full_image_ref_from_mops_env():
    with mock.patch.dict(
        os.environ,
        dict(MOPS_IMAGE_FULL_TAG="thdatascience.azurecr.io/ds/fooproj:test1"),
    ):
        assert "thdatascience.azurecr.io/ds/fooproj:test1" == std_find_image_full_tag("fooproj")


def test_make_image_ref_from_partial_version_env():
    with mock.patch.dict(os.environ, dict(FOOPROJ_VERSION="ds/fooproj:test1")):
        assert "thdatascience.azurecr.io/ds/fooproj:test1" == std_find_image_full_tag("fooproj")


def test_make_image_ref_from_tag_only_version_env():
    with mock.patch.dict(os.environ, dict(FOOPROJ_VERSION="test1")):
        assert "thdatascience.azurecr.io/ds/fooproj:test1" == std_find_image_full_tag("fooproj")


def test_get_full_image_ref_from_version_env_with_basename():
    with mock.patch.dict(os.environ, dict(FOOPROJ_VERSION="thdatascience.azurecr.io/ds/fooproj:test1")):
        assert "thdatascience.azurecr.io/ds/fooproj:test1" == std_find_image_full_tag(
            "fooproj", image_basename="ds/fooproj"
        )


def test_get_full_image_ref_from_version_env_without_basename():
    with mock.patch.dict(os.environ, dict(FOOPROJ_VERSION="thdatascience.azurecr.io/ds/fooproj:test1")):
        assert "thdatascience.azurecr.io/ds/fooproj:test1" == std_find_image_full_tag(
            "fooproj", image_basename=""
        )


def test_get_nothing_from_version_env_without_project_name():
    with mock.patch.dict(os.environ, dict(FOOPROJ_VERSION="thdatascience.azurecr.io/ds/fooproj:test1")):
        assert "" == std_find_image_full_tag()
