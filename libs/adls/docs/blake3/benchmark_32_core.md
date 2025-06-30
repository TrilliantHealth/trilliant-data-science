# this is why we want to use update_mmap rather than chunking into small pieces in Python:

The script used to create the below output is [here](./benchmark_hash_algos.py).

```text
root@ds-unified-directory-orch:/app/unified-directory# python ~/test_hash.py
creating a file with 33_554_432 bytes
File created: file-18.txt Size: 33_554_432
File: file-18.txt Size: 33_554_432 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 0.06032332499989934
+++ Time taken for blake3 hashing complete file of size 33_554_432: 0.007854590000079043
+++ Ratio best chunked time/avg full: 7.68
====================================================================
creating a file with 67_108_864 bytes
File created: file-19.txt Size: 67_108_864
File: file-19.txt Size: 67_108_864 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 0.1177412350002669
+++ Time taken for blake3 hashing complete file of size 67_108_864: 0.011758893999740394
+++ Ratio best chunked time/avg full: 10.0
====================================================================
creating a file with 134_217_728 bytes
File created: file-20.txt Size: 134_217_728
File: file-20.txt Size: 134_217_728 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 0.22944757000004756
+++ Time taken for blake3 hashing complete file of size 134_217_728: 0.019643506000193156
+++ Ratio best chunked time/avg full: 11.7
====================================================================
creating a file with 268_435_456 bytes
File created: file-21.txt Size: 268_435_456
File: file-21.txt Size: 268_435_456 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 0.43776513700004216
+++ Time taken for blake3 hashing complete file of size 268_435_456: 0.03689821199986909
+++ Ratio best chunked time/avg full: 11.9
====================================================================
creating a file with 536_870_912 bytes
File created: file-22.txt Size: 536_870_912
File: file-22.txt Size: 536_870_912 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 0.8812088589997984
+++ Time taken for blake3 hashing complete file of size 536_870_912: 0.0717327209999894
+++ Ratio best chunked time/avg full: 12.3
====================================================================
creating a file with 1_073_741_824 bytes
File created: file-23.txt Size: 1_073_741_824
File: file-23.txt Size: 1_073_741_824 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 1.7828068750000057
+++ Time taken for blake3 hashing complete file of size 1_073_741_824: 0.142340589000014
+++ Ratio best chunked time/avg full: 12.5
====================================================================
creating a file with 2_147_483_648 bytes
File created: file-24.txt Size: 2_147_483_648
File: file-24.txt Size: 2_147_483_648 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 3.538288104000003
+++ Time taken for blake3 hashing complete file of size 2_147_483_648: 0.2799733740002921
+++ Ratio best chunked time/avg full: 12.6
====================================================================
creating a file with 4_294_967_296 bytes
File created: file-25.txt Size: 4_294_967_296
File: file-25.txt Size: 4_294_967_296 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 7.087403635999635
+++ Time taken for blake3 hashing complete file of size 4_294_967_296: 0.5565005620001102
+++ Ratio best chunked time/avg full: 12.7
====================================================================
creating a file with 8_589_934_592 bytes
File created: file-26.txt Size: 8_589_934_592
File: file-26.txt Size: 8_589_934_592 Hash: 'blake3' Number of iterations for timing: 5
+++ Most efficient Chunk Size: 1_048_576 Time taken: 14.235944388000462
+++ Time taken for blake3 hashing complete file of size 8_589_934_592: 1.133229005999965
+++ Ratio best chunked time/avg full: 12.6
====================================================================
```
