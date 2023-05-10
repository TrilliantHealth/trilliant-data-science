# some code moved to thds.core.hashing


def nest(hashstr: str, split: int = 1) -> str:
    """A common pattern for formatting hash strings into directory
    paths for usability in various places. We borrow this pattern from
    DVC.

    Turns badbeef into b/adbeef.

    Default split is 1 because a human can fairly easily poke at 16
    directories if there's a debugging need to, for instance, count
    the number of total items. 256 (split=2) requires automation. And
    unlike DVC, we don't anticipate this being used for millions of
    'things', so each of the 16 top level directories will rarely
    contain more than several thousand items, which is pretty
    manageable for most systems.

    Another way to look at the split is to think about how many
    parallel list operations you'd like to be able to do. For most
    imaginable use cases, 16 parallel list operations would be
    plenty. If you think you'd need more - split at 2!
    """
    if split > 0 and split < len(hashstr):
        return f"{hashstr[:split]}/{hashstr[split:]}"
    return hashstr
