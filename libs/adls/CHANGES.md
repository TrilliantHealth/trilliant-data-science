## 1.2

- Automatically embed `MD5` sum in all files uploaded using the
  ADLSFileSystem utility. This also allows us to skip uploading a file
  if we detect that it's already been uploaded and has a matching
  checksum.
