this was run on my M1 MacBook Pro.

notably, update_mmap starts out faster than using Python mmap.mmap, but in larger files it doesn't matter
at all.

doing .update in smaller chunks is always much slower.

the inner loop in question is `hash_mmap` from [benchmark_hash_algos.py](./benchmark_hash_algos.py):

```python
        logger.info(f"DEBUG starting mmap for blake3")
        import mmap

        # hash_bytes = hasher.update_mmap(resolved_path).digest()
        with open(filepath, "rb") as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                hasher.update(mm)
                hash_bytes = hasher.digest()
```

```text
creating a file with 33_554_432 bytes
File created: file-18.txt Size: 33_554_432
File: file-18.txt Size: 33_554_432 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 0.049894000000000105
+++ Time taken for blake3 hashing using mmap: 0.057798792000000265
+++ Time taken for blake3 hashing complete file of size 33_554_432: 0.01612595799999994
+++ Ratio best chunked time/avg full: 3.09
====================================================================
creating a file with 67_108_864 bytes
File created: file-19.txt Size: 67_108_864
File: file-19.txt Size: 67_108_864 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 0.09987829099999956
+++ Time taken for blake3 hashing using mmap: 0.08525625000000048
+++ Time taken for blake3 hashing complete file of size 67_108_864: 0.033786707999999166
+++ Ratio best chunked time/avg full: 2.96
====================================================================
creating a file with 134_217_728 bytes
File created: file-20.txt Size: 134_217_728
File: file-20.txt Size: 134_217_728 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 0.1687718750000009
+++ Time taken for blake3 hashing using mmap: 0.07277087499999979
+++ Time taken for blake3 hashing complete file of size 134_217_728: 0.06168249999999986
+++ Ratio best chunked time/avg full: 2.74
====================================================================
creating a file with 268_435_456 bytes
File created: file-21.txt Size: 268_435_456
File: file-21.txt Size: 268_435_456 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 0.3473452499999965
+++ Time taken for blake3 hashing using mmap: 0.1431245000000061
+++ Time taken for blake3 hashing complete file of size 268_435_456: 0.11340679199999926
+++ Ratio best chunked time/avg full: 3.06
====================================================================
creating a file with 536_870_912 bytes
File created: file-22.txt Size: 536_870_912
File: file-22.txt Size: 536_870_912 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 0.7194241669999997
+++ Time taken for blake3 hashing using mmap: 0.2503625840000012
+++ Time taken for blake3 hashing complete file of size 536_870_912: 0.22509025000000804
+++ Ratio best chunked time/avg full: 3.2
====================================================================
creating a file with 1_073_741_824 bytes
File created: file-23.txt Size: 1_073_741_824
File: file-23.txt Size: 1_073_741_824 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 1.3447574580000037
+++ Time taken for blake3 hashing using mmap: 0.48448570900001187
+++ Time taken for blake3 hashing complete file of size 1_073_741_824: 0.4841062090000037
+++ Ratio best chunked time/avg full: 2.78
====================================================================
creating a file with 2_147_483_648 bytes
File created: file-24.txt Size: 2_147_483_648
File: file-24.txt Size: 2_147_483_648 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 2.8762159169999677
+++ Time taken for blake3 hashing using mmap: 1.0227993749999769
+++ Time taken for blake3 hashing complete file of size 2_147_483_648: 1.3003069579999647
+++ Ratio best chunked time/avg full: 2.21
====================================================================
creating a file with 4_294_967_296 bytes
File created: file-25.txt Size: 4_294_967_296
File: file-25.txt Size: 4_294_967_296 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 5.380822792000004
+++ Time taken for blake3 hashing using mmap: 1.7944092919999548
+++ Time taken for blake3 hashing complete file of size 4_294_967_296: 1.7909923750000871
+++ Ratio best chunked time/avg full: 3.0
====================================================================
creating a file with 8_589_934_592 bytes
File created: file-26.txt Size: 8_589_934_592
File: file-26.txt Size: 8_589_934_592 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 12.664286082999979
+++ Time taken for blake3 hashing using mmap: 3.6253420830000778
+++ Time taken for blake3 hashing complete file of size 8_589_934_592: 3.6221504169998298
+++ Ratio best chunked time/avg full: 3.5
====================================================================
```
