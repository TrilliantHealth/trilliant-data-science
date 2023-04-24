import re
from functools import reduce
from typing import NamedTuple

ADLS_SCHEME = (
    "adls://"  # this is our invention, but ADLS does not appear to define one suitable for general use.
)


def join(*parts: str) -> str:
    """For joining ADLS paths together."""

    def join_(prefix: str, suffix: str) -> str:
        prefix = prefix.rstrip("/")
        suffix = suffix.lstrip("/")
        return f"{prefix}/{suffix}"

    return reduce(join_, parts)


class AdlsRoot(NamedTuple):
    sa: str
    container: str

    def __str__(self) -> str:
        return format_fqn(*self)

    @classmethod
    def parse(cls, root_uri: str) -> "AdlsRoot":
        fqn = AdlsFqn.parse(root_uri)
        assert not fqn.path, f"URI {root_uri} does not represent an ADLS root!"
        return AdlsRoot(fqn.sa, fqn.container)

    def join(self, path: str) -> "AdlsFqn":
        return AdlsFqn(self.sa, self.container, join("", path))


class AdlsFqn(NamedTuple):
    """A fully-qualified ADLS path.

    Represents a (Storage Account, Container) root, if path is empty.

    Should usually be constructed via `parse`, `parse_fqn`, or `of`,
    which will perform validation.
    """

    sa: str
    container: str
    path: str

    @classmethod
    def of(cls, storage_account: str, container: str, path: str = "") -> "AdlsFqn":
        """Expensive but includes validation."""
        return AdlsFqn.parse(format_fqn(storage_account, container, path))

    @classmethod
    def parse(cls, fully_qualified_name: str) -> "AdlsFqn":
        return parse_fqn(fully_qualified_name)

    def __str__(self) -> str:
        return format_fqn(*self)

    def join(self, path_suffix: str) -> "AdlsFqn":
        return AdlsFqn(self.sa, self.container, join(self.path, path_suffix))

    def root(self) -> AdlsRoot:
        return AdlsRoot(self.sa, self.container)


SA_REGEX = re.compile(r"^[\w]{3,24}$")
# https://github.com/MicrosoftDocs/azure-docs/issues/64497#issuecomment-714380739
CONT_REGEX = re.compile(r"^\w[\w\-]{2,63}$")
# https://learn.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#container-names


def parse_fqn(fully_qualified_uri: str) -> AdlsFqn:
    """There are many ways to represent a fully qualified ADLS path, and most of them are cumbersome.

    This is an attempt to provide a standard way across our codebases
    that keeps all parts together, but allows separating them for
    passing into libraries.

    Because Storage Account names can only include alphanumeric
    characters, and Container names may only include alphanumerics
    plus the dash character, this simple format turns out to be
    unambiguous and easy for humans to read.

    We accept formatted strings with or without the leading forward
    slash in front of the path even though the formatter below
    guarantees the leading forward slash, but we do require there to
    be two spaces. If you wish to represent a Storage Account and
    Container with no path, simply append a forward slash to the end
    of your string, which represents the root of that SA and
    container, because a single forward slash is not valid as a path
    name for a blob in ADLS.
    """
    # an older, scheme-less version of format_fqn used spaces to separate sa and container.
    if fully_qualified_uri.startswith(ADLS_SCHEME):
        fully_qualified_uri = fully_qualified_uri[len(ADLS_SCHEME) :]
        sep = "/"
    else:
        sep = None
    sa, container, path = fully_qualified_uri.split(sep, 2)
    assert SA_REGEX.match(sa), sa
    assert CONT_REGEX.match(container), container
    return AdlsFqn(sa, container, path.lstrip("/"))


def format_fqn(storage_account: str, container: str, path: str = "") -> str:
    """Returns a fully-qualifed ADLS name in URI format, with adls:// as a prefix.

    When formatting, we will prefix your path with a forward-slash (/)

    if it does not already have one, in order to allow empty paths to
    be formatted and parsed simply.
    """

    assert SA_REGEX.match(storage_account), storage_account
    assert CONT_REGEX.match(container), container
    return f"{ADLS_SCHEME}{storage_account}/{container}/{path.lstrip('/')}"
