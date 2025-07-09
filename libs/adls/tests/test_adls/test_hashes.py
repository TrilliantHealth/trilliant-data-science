import os
import tempfile

from thds.adls import hashes


def test_basic_preferred_hash():
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        # make a biggish temp file
        temp_file.write(os.urandom(1024 * 1024 * 10))
        temp_file_path = temp_file.name
        # Calculate the hash using the preferred algorithm
        assert hashes.hash_cache.filehash(hashes.PREFERRED_ALGOS[0], temp_file_path) is not None
