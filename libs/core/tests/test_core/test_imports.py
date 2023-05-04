from unittest.mock import MagicMock

import pytest
from pytest_mock import MockFixture

from thds.core import imports

TEST_PACKAGE = "test_package"


@pytest.fixture
def mock_import_module(mocker: MockFixture) -> MagicMock:
    mock = mocker.patch.object(imports, "import_module", autospec=True)
    mock.return_value = None
    return mock


@pytest.fixture
def mock_get_base_package(mocker: MockFixture) -> MagicMock:
    mock = mocker.patch.object(imports, "get_base_package", autospec=True)
    mock.return_value = TEST_PACKAGE
    return mock


def test_try_imports(mock_import_module: MagicMock) -> None:
    imports.try_imports("installed_module1", "installed_module2")


def test_try_imports_w_missing_modules(mock_import_module: MagicMock) -> None:
    mock_import_module.side_effect = ImportError
    with pytest.raises(ImportError) as ex:
        imports.try_imports("missing_module1", "missing_module2")
        assert ex.match(r"^Install ['missing_module1', 'missing_module2']")


def test_try_imports_w_module_and_missing_modules(mock_import_module: MagicMock) -> None:
    mock_import_module.side_effect = ImportError
    with pytest.raises(ImportError) as ex:
        imports.try_imports("missing_module1", "missing_module2", module=TEST_PACKAGE)
        assert ex.match(rf"^Install ['missing_module1', 'missing_module2'] to use `{TEST_PACKAGE}`.")


def test_try_imports_w_module_and_missing_extra(
    mock_import_module: MagicMock, mock_get_base_package: MagicMock
) -> None:
    mock_import_module.side_effect = ImportError
    with pytest.raises(ImportError) as ex:
        imports.try_imports("missing_module1", "missing_module2", module=TEST_PACKAGE, extra="dev")
        assert ex.match(rf"^Install the 'dev' extra for `{TEST_PACKAGE}` to use `{TEST_PACKAGE}`.")
