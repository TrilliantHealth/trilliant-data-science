"""The reason for a hashed resource is that it enables worry-free caching.

If under any circumstances we re-use a name/URI with different bytes,
then having captured a hash will enable us to transparently detect the
situation and re-download.

It is strongly recommended that you construct these using `of`, as
that will avoid the accidental, invalid creation of an
AdlsHashedResource containing an empty hash.

How to get the hash itself?

From our experience, it seems that any file uploaded using Azure
Storage Explorer will have an MD5 calculated locally before upload and
that will be embedded in the remote file. You can look in the
properties of the uploaded file for Content-MD5 and copy-paste that
into whatever you're writing.

Programmatically, you can instead use `resource.upload`, which will
return to you an in-memory AdlsHashedResource object. If you want to
store it programmatically rather than in the source code, it's
recommended that you use `resource.to_path`, and then load it using
`resource.from_path`.

Prefer importing this module `as resource` or `from thds.adls
import resource`, and then using it as a namespace,
e.g. `resource.of(uri)`.
"""
from .core import AdlsHashedResource, from_source, get, of, parse, serialize, to_source  # noqa: F401
from .file_pointers import resource_from_path as from_path  # noqa: F401
from .file_pointers import resource_to_path as to_path  # noqa: F401
from .file_pointers import validate_resource as validate  # noqa: F401
from .up_down import get_read_only, upload  # noqa: F401
from .up_down import verify_or_create_resource as verify_or_create  # noqa: F401

AHR = AdlsHashedResource  # just an alias
