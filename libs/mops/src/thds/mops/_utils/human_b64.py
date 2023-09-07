import base64

import wordybin


def encode(the_bytes: bytes, num_bytes: int = 3) -> str:
    """The goal here is to allow people to easily read and remember
    the leading bytes of a string-encoded checksum.

    Previously, we put 256 bytes of entropy into 65 hex characters (with a slash).
    Hexadecimal strings are hard to remember even when you're just trying to
    keep track of the first few characters.

    Now, we're putting those same bytes into 53 chars, when num_bytes == 3,
    by switching to URL-safe base64 encoding, and representing the first triplet as a
    wordybin-encoded, human-readable string.

    By making num_bytes a multiple of 3, you can ensure that the 'end
    bytes' of the base64-encoded bytes will string-compare equal to the full b64 encoding
    of the raw bytes.
    """
    assert num_bytes > 0, "num_bytes must be > 0"
    return (
        wordybin.encode(the_bytes[:num_bytes])
        + "."
        # we use -_ instead of +/ to match URLsafe encodings
        + base64.b64encode(the_bytes[num_bytes:], altchars=b"-_").decode().rstrip("=")
        # base64 trailing == are meaningless and just take up space.
    ).strip(".")
    # if there was no wordybin part, we don't want a leading dot


def decode(the_str: str) -> bytes:
    """Unused by mops but here to formalize the procedure."""
    if "." in the_str:
        wordybin_part, b64_part = the_str.split(".")
        return wordybin.decode(wordybin_part) + base64.b64decode(b64_part + "==", altchars=b"-_")
    return wordybin.decode(the_str)
