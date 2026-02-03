#!/usr/bin/env python
# see https://stackoverflow.com/questions/17731660/hashlib-optimal-size-of-chunks-to-be-used-in-md5-update
# for where this Python script initially came from.
import contextlib
import mmap
import os
import random
import string
import tempfile
import timeit

from blake3 import blake3


@contextlib.contextmanager
def createdummyfiles():
    """
    Create a set of files at targetpath with random strings
    Outer for loop decides number of files with range specifying file size
    """
    with tempfile.TemporaryDirectory() as dir:

        def _():
            randomstring = "".join([random.choice(string.ascii_letters) for i in range(128)])
            for sizectr in range(18, 27):
                filename = f"file-{sizectr:_}.txt"
                fullfilename = os.path.join(dir, filename)
                with open(fullfilename, "w") as f:
                    print(f"creating a file with {128 * 2**sizectr:_} bytes")
                    for _ in range(2**sizectr):
                        f.write(randomstring)
                    print("File created: " + filename + f" Size: {os.path.getsize(fullfilename):_}")
                    yield filename, fullfilename

        yield _


def hashchunks(testfile, blk_size):
    filehash = blake3(max_threads=blake3.AUTO)
    with open(testfile, "rb") as f:
        while True:
            read_data = f.read(blk_size)
            if not read_data:
                break
            filehash.update(read_data)
    filehash.digest()


def hashcomplete(testfile):
    filehash = blake3(max_threads=blake3.AUTO)
    filehash.update_mmap(testfile)
    filehash.digest()


def memmap_hasher(testfile, hasher):
    with open(testfile, "rb") as f:
        with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
            hasher.update(mm)
            hasher.digest()


def blake3_mmap(testfile):
    memmap_hasher(testfile, blake3(max_threads=blake3.AUTO))


def xxhash_mmap(filepath):
    import xxhash

    memmap_hasher(filepath, xxhash.xxh3_64())


if __name__ == "__main__":
    result_list = []  # list (of lists) to record file stats

    with createdummyfiles() as dummy_files:
        for filename, fullfilename in dummy_files():
            result = []  # list to record stats of the file
            filesize = os.path.getsize(fullfilename)

            # initialize counters
            least_time = 100000000.0
            least_blk_size = 0

            num_iter = 5

            print(
                "File: {} Size: {:_} Hash: 'blake3' Number of iterations for timing: {}".format(
                    filename, filesize, num_iter
                )
            )
            result.append(filename)
            result.append(filesize)
            result.append(num_iter)
            # first try the hashing file by breaking it up into smaller chunks
            for ctr in range(6, 21):
                blk_size = 2**ctr
                funcstr = "hashchunks('{}', {:_})".format(fullfilename, blk_size)
                exec_time = timeit.timeit(
                    funcstr, setup="from __main__ import hashchunks", number=num_iter
                )
                if exec_time < least_time:
                    least_time = exec_time
                    least_blk_size = blk_size
            print(
                "+++ Most efficient Chunk Size: {:_} Time taken: {}".format(least_blk_size, least_time)
            )
            result.append(least_blk_size)
            result.append(least_time)

            funcstr = "blake3_mmap('{}')".format(fullfilename)
            timetaken_mmap = timeit.timeit(
                funcstr, setup="from __main__ import blake3_mmap", number=num_iter
            )
            print("+++ Time taken for blake3 hashing using mmap: {}".format(timetaken_mmap))
            result.append(timetaken_mmap)

            funcstr = "xxhash_mmap('{}')".format(fullfilename)
            timetaken_mmap = timeit.timeit(
                funcstr, setup="from __main__ import xxhash_mmap", number=num_iter
            )
            print("+++ Time taken for xxhash hashing using mmap: {}".format(timetaken_mmap))
            result.append(timetaken_mmap)

            # now try to hash the file all in one go
            funcstr = "hashcomplete('{}')".format(fullfilename)
            timetaken_complete = timeit.timeit(
                funcstr, setup="from __main__ import hashcomplete", number=num_iter
            )
            print(
                "+++ Time taken for blake3 hashing complete file of size {:_}: {}".format(
                    filesize, timetaken_complete
                )
            )
            print(f"+++ Ratio best chunked time/avg full: {least_time / timetaken_complete:.03}")
            result.append(timetaken_complete)
            print("====================================================================")
            result_list.append(result)

    for res in result_list:
        print(res)
